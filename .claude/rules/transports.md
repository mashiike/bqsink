---
paths:
  - "write.go"
  - "storagewrite.go"
  - "write_test.go"
  - "storagewrite_test.go"
  - "loadjobs_test.go"
  - "bqgcs/staging.go"
---

# 転送層

## `n < len(rows)` なら必ず non-nil error（最重要）

`RowWriter.WriteRows` は `io.Writer` の契約を写している。

```
// Write must return a non-nil error if it returns n < len(p).
// Implementations must not retain p.
```

**この不変条件を破っていたのが 2026-07-29 に修正した欠損バグ。** 当時の `loadJobsWriter` は `Append` でバッファに溜め、`flushLocked` が「失敗しても `buf.Reset()` する」形だったため、

```
状態を消す → 送る → 失敗する → リトライ → 消えてるので「やることがない」→ nil を返す
```

となり、**行が届いていないのに `nil` が返っていた**（`Sinker` が `gax.Invoke` で包んでいたので2回目の空振り nil が最終結果になった）。

バッファを Sinker から無くしたことで構造的に再発しない: `WriteRows` は渡された `rows` をその呼び出しの中で送り切り、`n` は 0 か `len(rows)` のどちらか。**`WriteRows` の中に「呼び出しをまたいで残る状態」を持ち込むと、この保証が壊れる。**

`n` は「書けた数」で「符号化できた数」ではない。符号化が途中で失敗したら `(0, err)`。

## ロードジョブも append も all-or-nothing

`AppendRowsResponse` の proto 定義（記憶ではなく原文）:

> If a request failed due to corrupted rows, **no rows in the batch will be appended.** The API will return row level error info, so that the caller can remove the bad rows and retry the request.

だから `n` が中途半端な値になる経路が無い。`RowError` は `Index`（リクエスト内の位置）と `Code` / `Message` を持つので、**壊れていた行を `Row.ID` で名指しできる**（`describeRowErrors`）。`GetResult` ではこの情報が取れないので `FullResponse` を使っている。

**リクエストを分割する実装（サイズ上限対応など）を入れるなら、そのときは `n` が真の prefix になるので、順番に送って最初の失敗で止めること。** 全部投げてから待つ形にすると、中間だけ失敗したときに `n` で表現できない。

## リトライは行を持っている層が持つ

`Sinker` は `Migrate` だけリトライする。書き込みは `RowWriter` の責務。**行を握っている層だけが再送できるから。**

| 層 | リトライ |
|---|---|
| `Sinker.Migrate` | `WithMigrationStrategy` の第2引数（nil なら1回だけ） |
| `loadJobsWriter.WriteRows` | `LoadJobs.RetryPolicy`（nil なら `DefaultRetryPolicy`） |
| `storageWriteWriter.WriteRows` | managedwriter の `EnableWriteRetries(true)` |

**`Sinker` が `WriteRows` を `retrying` で包んではいけない。** 包むと「writer が諦めた後にもう一度呼ぶ」ことになり、writer 側が状態を持っていた場合に上記のバグが再発する。

`retrying` はパッケージレベル関数で、`newRetryer == nil` なら op を1回呼ぶ。gax は回数制限を提供しないので `attemptLimiter` で足している。

## `EnableWriteRetries` は bqsink が有効化する

**以前は「二重リトライになるから渡さない」方針だったが、2026-07-29 に反転した。** `Sinker` が書き込みをリトライしなくなったので二重にならず、stream 再接続時の再 enqueue を知っているのはライブラリ側のコードだから。

`managedwriter` の doc.go:

> With write retries enabled, failed writes will be automatically attempted a finite number of times (currently 4) if the failure is considered retriable.
>
> Enabling retries is best suited for cases where users want to achieve at-least-once append semantics. Use of automatic retries may complicate patterns where the user is designing for exactly-once append semantics.

bqsink は at-least-once で exactly-once 未対応なので条件に合致する。

- **回数は4回固定でノブが無い。** `LoadJobs` には `RetryPolicy` があるのに `StorageWrite` には無い非対称はこれが理由
- 切りたい場合は `StorageWrite.DisableWriteRetries`。**「bqsink が有効化しない」という意味しか持てない** — `EnableWriteRetries(false)` は実装が `if enable` で分岐するだけの no-op で、**一度立った retryer を戻す手段がライブラリに無い**ため、利用者が `WriterOptions` に `EnableWriteRetries(true)` を入れていればそれは有効なまま。GoDoc にそう書いてある
- `AppendResult.TotalAttempts(ctx)` は `(int, error)` を返す。ログに出すには error 処理が必要になるので今は出していない

## `writerOptions` の順序は仕様

利用者の `WriterOptions` を先に適用し、その後で bqsink が destination table・schema descriptor・`EnableWriteRetries` を設定する。**`WriterOption` は `func(*ManagedStream)` なので後勝ち。** relation と宣言スキーマが source of truth で、行が着かないことを避けるのがこのライブラリの目的なので、それらと矛盾する指定は無効化される。

`StreamName` が設定されているときは `WithDestinationTable` を付けない。既存ストリームは既にテーブルに紐づいているため。

## writer は状態を持たないのでロックが無い

`loadJobsWriter` も `storageWriteWriter` も、変化するフィールドを持っていない（`loader` / `schema` / `retryPolicy` / `logger`、または `client` / `stream` / `descriptor` / `logger`）。**だから `sync.Mutex` が無い。** `ManagedStream` 自体が concurrent safe。

以前は `loadJobsWriter` がバッファを持っていたためロックがあり、**`Append` が閾値に達するとロードジョブの完了まで（秒〜分）ロックを保持して並行する `Sink` を全部待たせていた。** その問題も消えた。代わりに**並行する `Sink` が並行してロードジョブを投入する**ので、ジョブ数 quota は利用者のバッチサイズ次第になる。

**フィールドを足すときは「呼び出しをまたいで変化するか」を確認する。** 変化するなら、ロックの要否と `n` の保証の両方を考え直すことになる。

## ログの level は「呼び出し元に返るか」で決まる

**`Error` は使わない。** エラーは戻り値で返しているので、ログにも出すと二重処理になる（`~/.claude/rules/go.md`）。

`Warn` は**返らずに捨てている事象専用**。転送層でそれに当たるのは2箇所。

| 箇所 | 捨てているもの |
|---|---|
| `stagedLoader.load` の cleanup | ステージングしたオブジェクトの削除失敗。行はもうロード済み／報告済み |
| `storageWriteWriter.Close` | stream の close が失敗しているときの client close 失敗 |

**`_ = err` を書きたくなったら、それは Warn ログの場所。**

`Info` は load job の投入と完了だけ。**投入時にもログを出しているのは、`job.Wait` が数秒〜数分ブロックするので、終わってからしか記録がないと止まって見えるため。**

`Debug` は `loadJobsWriter` の "loading rows"、`storageWriteWriter` の "appended rows"、stream を開いたとき。

`RowWriter` を直接構造体リテラルで作るテストは `logger` を埋めること。`Open` の契約で non-nil なので**内部では nil チェックしていない**（`loadjobs_test.go` の直接構築で一度 panic させた）。

## Storage Write API の proto 型は素直ではない

`adapt` が BQ スキーマから作る proto の型（実測）:

```
STRING     → string      TIMESTAMP  → int64   （マイクロ秒）
INTEGER    → int64       DATE       → int32   （epoch からの日数）
FLOAT      → double      TIME       → int64
BOOL       → bool        DATETIME   → int64   ← エンコード方法が公開されていない
BYTES      → bytes       NUMERIC    → bytes   ← BigDecimal のバイナリ表現
JSON       → string      BIGNUMERIC → bytes   ← 同じ
```

**`adapt` はスキーマ変換しか提供しない。値のエンコードは利用者の責任。** DATETIME の int64 表現は `CivilTimeEncoder`、NUMERIC の bytes は `BigDecimalByteStringEncoder` が必要で、どちらも Go クライアントには無い（Java にはある）。

## `WithProtoMapping` で string に逃がしている

BigQuery が受け付ける proto 型は複数ある（公式ドキュメントの表を原文で確認済み）。

```
NUMERIC, BIGNUMERIC: int32, int64, uint32, uint64, double, float, string / bytes
DATETIME, TIME:      string（リテラル）/ int64（CivilTimeEncoder 経由）
DATE:                int32（推奨）, int64, string
TIMESTAMP:           int64（推奨）, int32, uint32, google.protobuf.Timestamp   ← string 不可
```

`textualTypes` の5つを string にマップすることで、**エンコーダを自作せずに済み、かつ Load Jobs と同じテキスト表現が使える**。TIMESTAMP だけ string が使えないので int64 マイクロ秒のまま。

**この表を「TIME は string only」と読んだ要約に一度騙されている。** ドキュメントが DATETIME と TIME を同じ行にまとめていて、要約が `int64` を落としていた。`adapt` の実装（TIME → int64）と矛盾して見えたのが手がかり。型マッピングの表は原文を読む。

## `ManagedStream.Close()` が返す `io.EOF` は正常終了

ソースにそう書いてある。

```go
// For normal operation, mark the stream error as io.EOF.
if ms.err == nil { ms.err = io.EOF }
```

**これを見落として統合テストが落ちた。** `errors.Is(serr, io.EOF)` で無視している。

`Close` は stream と client の**両方**を閉じる。片方だけだと gRPC 接続が漏れる。

`Sinker.Close` は writer の `Close` を呼ぶだけ。**バッファが無いので flush する必要がなく、以前あった「`Sinker.Close` が先に `Flush` を呼ぶ」構造は不要になった。**

## `PendingStream` / `BufferedStream` を `Validate` で拒否している

`Open → AppendRows → Close` のライフサイクルでは、この2つは**行が永久に見えないまま成功を返す**。

- `PendingStream` は `Finalize` + `Client.BatchCommitWriteStreams` が必要
- `BufferedStream` は `FlushRows(offset)` が必要

silently lossy なオプションを残すより拒否する方を選んだ。

## `Stager` を interface にした理由

`bqgcs` を別パッケージにすることで、**bqsink を import しただけでは `cloud.google.com/go/storage` がコンパイル対象に入らない**。GCS を使わない利用者に依存を負わせないため。

`Stager` が `Validator` を実装していれば `LoadJobs.Validate` から呼ばれるので、バケット未指定などは `New` の時点で弾ける。

`bqgcs.Staging` のオブジェクト名は `ナノ秒-連番.json`。並行する writer とリトライで衝突しないようにしている。

## バッファリングは呼び出し側の仕事

`Sinker` は溜めない。`Sink(ctx, vs...)` に渡された行がそのままバッチになる。**`LoadJobs` に1行ずつ渡すとロードジョブが1本ずつ立つ**ので、GoDoc と README で明示している。

`FlushRows` / `FlushBytes` / `DefaultFlushRows` / `DefaultFlushBytes` は削除した。どれも「未確認の値」と GoDoc に書いてあった推測値で、`FlushBytes` はそもそも要求されていないのに足されたもの。

将来 `BufferedSinker`（`bufio.NewWriter` 相当で `Sinker` を外から包む型）を足す余地はある。**そのときの罠**: `FillRow` は `Sinker.Sink` の中で呼ばれるので、`T` を溜めると `_ingestion_at` が「`Sink` を呼んだ時刻」ではなく flush 時刻になり、`IngestionMetadata.IngestionAt` の GoDoc と食い違う。
