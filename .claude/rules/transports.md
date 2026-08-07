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

- **結果はバッファの状態ではなく「取り出した瞬間のバッチ」に紐づく。** 閾値到達でも `FlushRows` でも、投入するのは `batch := w.buf; w.buf = nil` で `w.buf` から切り離した後の値で、投入した行が再び `w.buf` を経由することはない。「もう一度呼んだらバッファが空だった」という経路が存在しない
- **1つの投入は1回しか起こらない。** 切り出しと `wg.Add` が同じロック区間の中にあるので、閾値到達を跨いで積まれた行を2つの呼び出しが同時に取り出すことはない。投入の失敗は呼び出し元には返らず `pending` に積まれ、次の `FlushRows` / `Close` が一度だけ報告する
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
| `LoadJobsWriter.load`（ジョブ投入） | `LoadJobs.RetryPolicy`（nil なら `DefaultRetryPolicy`） |
| `StorageWriter.WriteRows` | managedwriter の `EnableWriteRetries(true)` |

**`Sinker` が `WriteRows` / `WriteResult.Wait` を `retrying` で包んではいけない。** 包むと「writer が諦めた後にもう一度呼ぶ」ことになり、上記のバグが再発する。

リトライは `load` の中（`w.loader.load` を包む形）にある。**バッチは投入前に `w.buf` から切り離されるので、リトライ中に別の goroutine が同じ行を二重に取り出すことはない。**

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
| `buf` / `pending` / `closed`（LoadJobs） | `WriteRows` / `FlushRows` / `Close` |

**ロック下で読んだ値をローカル変数に取り出してから I/O する。** `StorageWriter.WriteRows` が `stream` / `schema` / `descriptor` をまとめて取り出しているのはこのため（`marshalStorageWriteRow` が自由関数で、writer を参照しないのも同じ理由）。ロック外で `w.schema` を読むと `BindSchema` と競合する。

**例外: `StorageWriter.BindSchema` は関数全体でロックを保持する**（ネットワーク呼び出し中も）。「bind 済みか」を確認してから解放し、開いてから再ロックする形にすると、**並行する2つの `BindSchema` が両方ともクライアントとストリームを開いてリークする**。1回しか呼ばれない契約なので保持コストは問題にならない。

**`Close` 後に呼ばれても同じようにリークするバグが既存にあった**（`closed` を確認していなかった）。修正済みで、今は関数の先頭でロックを取った直後に `closed` を確認し、閉じていれば何も開かずに返る。

以前は「writer は状態を持たないのでロックが無い」と書いてあり、**閾値到達時にジョブ完了までロックを握って並行 `Sink` を全部待たせる**という過去の問題もここに記録されていた。今のバッファはその問題を避けている（後述の「バッファがある writer の安全装置」を参照）。

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

**`Wait` を通らない転送層のテストはジョブの投入まで検証していない。** `flushRows == 0` の `LoadJobsWriter.WriteRows` と `StorageWriter.WriteRows` はそこが変わらない: 投入は `WriteRows` の中だが、結果の検査は返した `WriteResult` の `Wait` 経由。

**`flushRows` を設定した `LoadJobsWriter.WriteRows` はここが逆転する。** 返す `WriteResult` は受理を約束するだけなので、`Wait` は常に `(len(rows), nil)` を返し、ジョブの成否を一切運ばない。そのジョブを検証するテストは `Wait` ではなく `FlushRows` か `Close` の戻り値を見る必要がある。

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

**2026-08-05 に方針が変わった。** 以前は「バッファリングは呼び出し側の仕事」で `FlushRows` は削除されていたが、E案で `LoadJobs.FlushRows` として転送層に戻した。**根拠は「ジョブ数 quota とジョブ単位のオーバーヘッドは LoadJobs 固有の経済性であって、転送に依らない性能アドオンではない」。** だから `StorageWrite` 側にバッファは無く、`FlushRows` も `LoadJobsWriter` にしか生えていない。この非対称は意図的。

**却下した形**: `Sinker` の外に汎用の `BufferedWriter` デコレータを置く案（`bufio` と同型）。StorageWrite にも巻けてしまい、ドメインと乖離する。`Sinker` に `Flush` を生やす案も却下 — writer を作るのは呼び出し側なので、手元の writer に直接 `FlushRows` を呼べる。

### なぜ per-call 配送をやめたか

E案（2026-08-05）は「バッファ×呼び出しごとの配送約束」を両立させようとしていた。`WriteRows` はバッファに乗せるだけで返るのに、返す `WriteResult` は依然として**配送**（ジョブが実際に終わった結果）を約束していて、その約束を果たすのは `Wait` の役目だった。**`Wait` は「結果を知る」ことと「投入を強制する」ことを兼ねていた。**

半端に埋まったバッファの前ではこの二重役割は両立しない。閾値に達していなくても `Wait` が呼ばれたら、配送を約束した以上そこで投入するしかない。`Sinker.Sink` は `WriteRows` の直後に `Wait` を呼ぶので、**呼ぶたびに閾値未達のバッファを強制的に吐き出す**ことになり、バッファがあってもジョブは呼び出しごとに1本のままだった。

今の設計は約束の対象を層で分けてこれを解いた（`WriteResult` の GoDoc 参照）。`FlushRows` を設定した `LoadJobsWriter.WriteRows` は**受理**だけを約束し、その `WriteResult.Wait` は常に即座に解決済みの `(len(rows), nil)` を返す——投入を強制しない。**配送**の約束は `FlushRows` や `Close` が返す別の `WriteResult` に移した。「結果を知る」と「投入を強制する」を同じ `Wait` に持たせない、という一点が矛盾を解いている。

### `FlushRows` の効きどころ（GoDoc に書いた内容の根拠）

**単一 goroutine + 同期 `Sink` でも、今はバッファが効く。** 前節の通り、閾値未達の `WriteRows` は投入せずに受理だけを返すので、`Sinker.Sink` が毎回 `Wait` を呼んでもそれは投入を強制しない。行は `w.buf` に積まれ続け、閾値に達した呼び出しだけがジョブを1本投入する。**単一 goroutine の逐次 `Sink` でジョブが減らなかったのが、この再設計の主目的の1つ。**

それでも次の場面には別の意味がある:

- **`FlushRows` を明示的に呼ぶとき**（ticker や shutdown 前の drain など）。閾値未達の行を持ち越したくないときに使う
- **複数 goroutine が同時に書くとき。** 閾値到達を跨いで別の呼び出しの行が同じバッファに積まれるので、各呼び出しは自分の行数分の受理をすぐ返せる一方、ジョブは1本にまとまる
- **`Wait` を後回しにするとき**（`Sinker` を経由せず writer を直接使う場合）

**「バッファは受理までしか約束しない」ことを見落としてテストを書かない。** `flushRows` を設定した `WriteRows` が返す `WriteResult` の `Wait` は、前節の通りジョブの成否を一切運ばない。ジョブの結果を検証するテストは `FlushRows` か `Close` の戻り値を見る。

### バッファがある writer の安全装置

1. **閾値に達したバッファは、それを満たした `WriteRows` がその場で同期的に投入する**（`bufio.Write` がバッファ満杯で flush するのと同じ）。投入前に `batch := w.buf; w.buf = nil` で切り離すので、保持する行が無限に増えない
2. **`Close` は `wg.Wait()` で「`WriteRows` の閾値投入」と「`FlushRows` 自身の投入」の両方が終わるのを待ってから、初めて `pending` を読む。** この順序が要点: `WriteRows` は投入が失敗したら `pending` に積んでから（`defer` の）`wg.Done()` を呼ぶので、`Close` が待った後に読む `pending` には、待っている間に終わった投入の失敗も必ず反映されている。`Close` が報告するのは、その `pending` と、`Close` 自身がバッファの残りを投入した結果の2つだけ
3. **`FlushRows` 自身が投入した分の結果は `pending` に一切乗らない。** `FlushRows` は自分の投入だけ `wg.Add` してその場で走らせ、`wg.Wait()` はしない。だから同時に走っている `WriteRows` 発の投入の結末は次の `FlushRows` か `Close` に残るが、**`FlushRows` 自身の投入結果を回収できるのはその呼び出しが返す `WriteResult` を `Wait` することだけで、それをしないと `Close` を含め誰にも届かない**
4. **失敗したバッチの行は捨てる**（retain-until-delivered にしない）。保持し続けると head-of-line blocking と無限成長になる。行を持っているのは呼び出し側なので再送はそちらの判断
5. **`wg.Add` は必ず `closed` 検査と同じロック区間の中で呼ぶ**（`WriteRows` の閾値到達時・`FlushRows` 自身の投入時のいずれも）。`Close` は `closed = true` をロック下で立ててから `wg.Wait()` するので、この規律により `Add` は必ず `Wait` より前に起こる（`sync.WaitGroup` が要求する順序）。ロック区間の外に `Add` を出すと `Close` の `Wait` と競合し得る

### 共有バッファの ctx は「投入した呼び出し」のもの（設計上の制約）

`WriteRows` が閾値到達で投入するときも、`FlushRows` がバッファの残りを投入するときも、使うのは**その投入を行った呼び出し自身の ctx**。**その ctx がキャンセルされると、同じバッファに同居している（先に呼ばれて積まれただけで自分では投入しなかった）他の呼び出しの行まで投入失敗の対象になる。** それらの呼び出し自身の `WriteResult` はすでに受理として解決済みなので変わらないが、投入失敗は `pending` に積まれ、次の `FlushRows` か `Close` が報告するまで誰にも見えない。「自分は関与していない ctx のキャンセルで自分の行が失敗の対象になる」という制約自体は、報告の経路が per-call の `Wait` から `pending` 経由に変わっても解消していない。

- `FlushRows == 0` ならバッファが共有されないので発生しない
- 解消するには投入を goroutine に切り出して ctx を分離する必要があり、「goroutine を持たない」という現設計の前提を変えることになる。**未対応。** `SinkAsync`（2026-08-06 追加）は `Wait` を呼ぶタイミングを呼び出し側に委ねるだけで、投入自体は依然として `WriteRows` の中で同期的に走るので、この ctx 共有は解消していない

**ロックはジョブの実行中に保持しない。** バッチを `w.buf` から切り離してからロックを離し、その後で投入する。以前バッファがあった頃は「閾値到達時にジョブ完了まで（秒〜分）ロックを握って並行する `Sink` を全部待たせる」問題があった。

**`FillRow` は `Sink` の中で呼ばれるので、`_ingestion_at` は投入時刻ではなく `Sink` を呼んだ時刻のまま。** バッファが writer 側（変換済みの `[]Row` を溜める）にあるおかげで、`BufferedSinker` 案にあった罠（flush 時刻になる）を踏んでいない。**バッファを `Sinker` 側に移そうとしたらここが壊れる。**
