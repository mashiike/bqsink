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

契約は `io.Writer` の写しだが、**`n` を返すのは `WriteRows` ではなく `WriteResult.Wait`** になった（2026-08-05 の E案）。

```go
WriteRows(ctx, rows) (WriteResult, error)   // error は「行を受け取らなかった」ときだけ
WriteResult.Wait(ctx) (n int, err error)    // n < len(rows) なら必ず non-nil error
```

`n` は「書けた数」で「符号化できた数」ではない。符号化が途中で失敗したら `(0, err)`。

### 2026-07-29 の欠損バグと、いま再発しない理由

当時の `loadJobsWriter` はバッファを持ち、`flushLocked` が「失敗しても `buf.Reset()` する」形だった。

```
状態を消す → 送る → 失敗する → リトライ → 消えてるので「やることがない」→ nil を返す
```

成分は2つ。(1) 失敗しても状態を捨てる flush、(2) `Sinker` が `gax.Invoke` で `WriteRows` を包んでいたので**2回目の空振り nil が最終結果になった**。

**バッファは 2026-08-05 に `LoadJobsWriter` へ戻したが、成分の両方が構造的に消えている。**

- **結果はバッファの状態ではなく「バッチ」に紐づく。** `loadResult` は自分の行が入った `*loadBatch` を握っていて、`Wait` はそのバッチの結末を読む。「もう一度呼んだらバッファが空だった」という経路が存在しない
- **1つのバッチは1回だけ投入される**（`claimed atomic.Bool` の CAS）。勝者が投入し、他の待ち手は `done` チャネルで同じ結末を受け取る
- **`Sinker` は今も `WriteRows` / `Wait` をリトライで包まない**（下記「リトライは行を持っている層が持つ」）

**`WriteRows` に状態を足すときは、その状態ではなく結果の紐づけ先を確認すること。** バッチに紐づいている限り安全で、バッファの有無を見て分岐し始めたら危険。

## ロードジョブも append も all-or-nothing

`AppendRowsResponse` の proto 定義（記憶ではなく原文）:

> If a request failed due to corrupted rows, **no rows in the batch will be appended.** The API will return row level error info, so that the caller can remove the bad rows and retry the request.

だから `n` が中途半端な値になる経路が無い。`RowError` は `Index`（リクエスト内の位置）と `Code` / `Message` を持つので、**壊れていた行を `Row.ID` で名指しできる**（`describeRowErrors`）。`GetResult` ではこの情報が取れないので `FullResponse` を使っている。

**リクエストを分割する実装（サイズ上限対応など）を入れるなら、そのときは `n` が真の prefix になるので、順番に送って最初の失敗で止めること。** 全部投げてから待つ形にすると、中間だけ失敗したときに `n` で表現できない。

## リトライは行を持っている層が持つ

`Sinker` はマイグレーションだけリトライする。書き込みは `RowsWriter` の責務。**行を握っている層だけが再送できるから。**

| 層 | リトライ |
|---|---|
| `Sinker.start`（マイグレーション） | `WithMigrationStrategy` の第2引数（nil なら1回だけ） |
| `LoadJobsWriter.submit`（ジョブ投入） | `LoadJobs.RetryPolicy`（nil なら `DefaultRetryPolicy`） |
| `StorageWriter.WriteRows` | managedwriter の `EnableWriteRetries(true)` |

**`Sinker` が `WriteRows` / `WriteResult.Wait` を `retrying` で包んではいけない。** 包むと「writer が諦めた後にもう一度呼ぶ」ことになり、上記のバグが再発する。

リトライは `submit` の中（`w.load` を包む形）にある。**バッチ単位でリトライされ、成功しても失敗してもその結末が `done` の閉鎖で全待ち手に伝わる**ので、リトライ中に別の goroutine が同じバッチを二重投入することはない。

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

## writer は状態を持つのでロックがある（2026-08-05 に反転）

`BindSchema` / `BindLogger` が後から状態を入れる形になり、`LoadJobsWriter` はバッファも持つので、**両 writer が `sync.Mutex` を持っている。**

| フィールド | 誰が書くか |
|---|---|
| `logger` | `BindLogger`（`NewSinker` から1回） |
| `schema` / `descriptor` / `stream` | `BindSchema`（初回 `Sink` から1回） |
| `open` / `failed` / `closed`（LoadJobs） | `WriteRows` / `Flush` / `Close` |

**ロック下で読んだ値をローカル変数に取り出してから I/O する。** `StorageWriter.WriteRows` が `stream` / `schema` / `descriptor` をまとめて取り出しているのはこのため（`marshalStorageWriteRow` が自由関数で、writer を参照しないのも同じ理由）。ロック外で `w.schema` を読むと `BindSchema` と競合する。

**例外: `StorageWriter.BindSchema` は関数全体でロックを保持する**（ネットワーク呼び出し中も）。「bind 済みか」を確認してから解放し、開いてから再ロックする形にすると、**並行する2つの `BindSchema` が両方ともクライアントとストリームを開いてリークする**。1回しか呼ばれない契約なので保持コストは問題にならない。

以前は「writer は状態を持たないのでロックが無い」と書いてあり、**閾値到達時にジョブ完了までロックを握って並行 `Sink` を全部待たせる**という過去の問題もここに記録されていた。今のバッファはその問題を避けている（前節の3つの安全装置を参照）。

**フィールドを足すときは「呼び出しをまたいで変化するか」と「I/O 中に保持するか」を確認する。**

## ログの level は「呼び出し元に返るか」で決まる

**`Error` は使わない。** エラーは戻り値で返しているので、ログにも出すと二重処理になる（`~/.claude/rules/go.md`）。

`Warn` は**返らずに捨てている事象専用**。転送層でそれに当たるのは2箇所。

| 箇所 | 捨てているもの |
|---|---|
| `stagedLoader.load` の cleanup | ステージングしたオブジェクトの削除失敗。行はもうロード済み／報告済み |
| `StorageWriter.Close` | stream の close が失敗しているときの client close 失敗 |

**`_ = err` を書きたくなったら、それは Warn ログの場所。**

`Info` は load job の投入と完了だけ。**投入時にもログを出しているのは、`job.Wait` が数秒〜数分ブロックするので、終わってからしか記録がないと止まって見えるため。**

`Debug` は `LoadJobsWriter` の "loading rows"、`StorageWriter` の "appended rows"、stream を開いたとき。

**writer を直接構造体リテラルで作るテストは `logger` を埋めること。** `NewWriter` が `slog.DiscardHandler` で初期化し `BindLogger` が差し替える契約なので、**内部では nil チェックしていない**（`loadjobs_test.go` の直接構築で一度 panic させた）。

**`Wait` を通らない転送層のテストは書けない。** `WriteRows` は `WriteResult` を返すだけなので、`Wait` を呼ばないテストはジョブの投入自体を検証していない（`LoadJobsWriter` は `flushRows == 0` でも投入は `WriteRows` の中だが、結果の検査は `Wait` 経由）。

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

**`Sinker.Close` は存在しない**（2026-08-05 に削除）。writer を作るのは呼び出し側なので、閉じるのも呼び出し側。`json.Encoder` に `Close` が無いのと同じ配置で、`Sinker` は溜めないので閉じるものを持っていない。

**`StorageWriter.Close` は待たれなかった `WriteResult` の成否を報告しない。** `LoadJobsWriter.Close` は報告する（自分が行を保持しているから）。この非対称は意図的で、GoDoc にも書いてある。

## `PendingStream` / `BufferedStream` を `Validate` で拒否している

`Open → AppendRows → Close` のライフサイクルでは、この2つは**行が永久に見えないまま成功を返す**。

- `PendingStream` は `Finalize` + `Client.BatchCommitWriteStreams` が必要
- `BufferedStream` は `FlushRows(offset)` が必要

silently lossy なオプションを残すより拒否する方を選んだ。

## `Stager` を interface にした理由

`bqgcs` を別パッケージにすることで、**bqsink を import しただけでは `cloud.google.com/go/storage` がコンパイル対象に入らない**。GCS を使わない利用者に依存を負わせないため。

`Stager` が `Validator` を実装していれば `LoadJobs.Validate` から呼ばれるので、バケット未指定などは `LoadJobs.NewWriter` の時点で弾ける。

`bqgcs.Staging` のオブジェクト名は `ナノ秒-連番.json`。並行する writer とリトライで衝突しないようにしている。

## バッファは `LoadJobsWriter` にあり、`Sinker` には無い

**2026-08-05 に方針が変わった。** 以前は「バッファリングは呼び出し側の仕事」で `FlushRows` は削除されていたが、E案で `LoadJobs.FlushRows` として転送層に戻した。**根拠は「ジョブ数 quota とジョブ単位のオーバーヘッドは LoadJobs 固有の経済性であって、転送に依らない性能アドオンではない」。** だから `StorageWrite` 側にバッファは無く、`Flush` も `LoadJobsWriter` にしか生えていない。この非対称は意図的。

**却下した形**: `Sinker` の外に汎用の `BufferedWriter` デコレータを置く案（`bufio` と同型）。StorageWrite にも巻けてしまい、ドメインと乖離する。`Sinker` に `Flush` を生やす案も却下 — writer を作るのは呼び出し側なので、手元の writer に直接 `Flush` を呼べる。

### `FlushRows` の効きどころ（GoDoc に書いた内容の根拠）

`Sinker.Sink` は同期で、内部で `WriteResult.Wait` を即座に呼ぶ。**`Wait` は自分のバッチを投入するので、1つの goroutine が順番に `Sink` する限りジョブは呼び出しごとに1本のまま**（バッファは効かない）。効くのは:

- **複数 goroutine が同時に書くとき。** 誰かが `Wait` する前に同じバッチへ入った行が1本のジョブで運ばれる。しかも各呼び出しは自分の結末を失わない（バッチの結果を全員が受け取る）
- **`Flush` を明示的に呼ぶとき**（ticker など）
- **`Wait` を後回しにするとき**（`Sinker` を経由せず writer を直接使う場合）

**「バッファすればジョブが減る」と書いてはいけない。** 単一 goroutine + 同期 `Sink` では減らない。減らしたいなら将来 `SinkAsync` 的な非同期の顔を足す（非破壊で足せる位置に置いてある）。

### バッファがある writer の安全装置

1. **閾値に達したバッチは、それを満たした `WriteRows` が同期的に投入する**（`bufio.Write` がバッファ満杯で flush するのと同じ）。だから保持する行が無限に増えない
2. **`Close` が3つを報告する**: 保留中の行の投入結果、**誰かが `Wait` していない失敗**、そして**閉じる時点で走っているジョブの結末**
3. **失敗したバッチの行は捨てる**（retain-until-delivered にしない）。保持し続けると head-of-line blocking と無限成長になる。行を持っているのは呼び出し側なので再送はそちらの判断

**`loadBatch.unread` は「バッチ単位のフラグ」ではなく「まだ結末を受け取っていない呼び出しの数」。** ここを1ビットにすると、**3つの `Sink` が同じバッチを共有していて1つだけが `Wait` した場合に、残り2つの失敗が握り潰される**（`Close` が「既読」と見なして落とす）。`WriteRows` が結果を作るたびに `+1`、各 `loadResult.Wait` が初回だけ `-1`。**`Close` は `unread > 0` のバッチだけ報告する。** 2026-08-05 のレビューで、1ビット版のこの欠落を指摘されて直した。

**`Close` は `inflight` が 0 になるまで待つ**（`sync.Cond`）。待たないと、別の goroutine が投入したジョブが**まだ走っている間に `Close` が `failed` を覗いて何も見つけず nil を返し**、その後で追記された失敗が誰にも届かない。`Cond` は `settled()` で遅延生成する（テストが構造体リテラルで writer を組むため）。

**`Close` は2回目以降も `failed` を見る。** 早期 `return nil` を置くと上記の穴が復活する。

### 共有バッチの ctx は「投入した呼び出し」のもの（設計上の制約）

`submit` は CAS に勝った呼び出しの ctx でジョブを実行する。**その ctx がキャンセルされると、同じバッチに同居している他の呼び出しの行まで失敗として報告される。** `WriteResult.Wait` の GoDoc に明記した。

- `FlushRows == 0` ならバッチは共有されないので発生しない
- 解消するには投入を goroutine に切り出して ctx を分離する必要があり、「goroutine を持たない」という現設計の前提を変えることになる。**未対応**（`SinkAsync` と一緒に検討する）

**ロックはジョブの実行中に保持しない。** バッチを `w.open` から外してからロックを離し、その後で投入する。以前バッファがあった頃は「閾値到達時にジョブ完了まで（秒〜分）ロックを握って並行する `Sink` を全部待たせる」問題があった。

**`FillRow` は `Sink` の中で呼ばれるので、`_ingestion_at` は投入時刻ではなく `Sink` を呼んだ時刻のまま。** バッファが writer 側（変換済みの `[]Row` を溜める）にあるおかげで、`BufferedSinker` 案にあった罠（flush 時刻になる）を踏んでいない。**バッファを `Sinker` 側に移そうとしたらここが壊れる。**
