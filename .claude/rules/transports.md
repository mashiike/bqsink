---
paths:
  - "write.go"
  - "storagewrite.go"
  - "write_test.go"
  - "storagewrite_test.go"
  - "bqgcs/staging.go"
---

# 転送層

## ログの level は「呼び出し元に返るか」で決まる

**`Error` は使わない。** エラーは戻り値で返しているので、ログにも出すと二重処理になる（`~/.claude/rules/go.md`）。

`Warn` は**返らずに捨てている事象専用**。転送層でそれに当たるのは3箇所しかない。

| 箇所 | 捨てているもの |
|---|---|
| `stagedLoader.load` の cleanup | ステージングしたオブジェクトの削除失敗。行はもうロード済み／損失報告済み |
| `storageWriteWriter.Flush` | 2件目以降の reject。戻り値は `firstErr` 1件だけ |
| `storageWriteWriter.Close` | 既に失敗があるときの stream / client close 失敗 |

**`_ = err` を書きたくなったら、それは Warn ログの場所。** 逆に、error に情報を含めて返しているもの（`flushLocked` の「N 行を捨てた」）はログにしない。

`Info` は load job の投入と完了だけ。**投入時にもログを出しているのは、`job.Wait` が数秒〜数分ロックを持ったままブロックするので、終わってからしか記録がないと止まって見えるため。**

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

## `writerOptions` の順序は仕様

利用者の `WriterOptions` を先に適用し、その後で bqsink が destination table と schema descriptor を設定する。**`WriterOption` は `func(*ManagedStream)` なので後勝ち。** relation と宣言スキーマが source of truth なので、それと矛盾する指定は無効化される。

`StreamName` が設定されているときは `WithDestinationTable` を付けない。既存ストリームは既にテーブルに紐づいているため。

## `ManagedStream.Close()` が返す `io.EOF` は正常終了

ソースにそう書いてある。

```go
// For normal operation, mark the stream error as io.EOF.
if ms.err == nil { ms.err = io.EOF }
```

**これを見落として統合テストが落ちた。** `errors.Is(serr, io.EOF)` で無視している。

`Close` は stream と client の**両方**を閉じる。片方だけだと gRPC 接続が漏れる。

## `PendingStream` / `BufferedStream` を `Validate` で拒否している

`Open → AppendRows → Close` のライフサイクルでは、この2つは**行が永久に見えないまま成功を返す**。

- `PendingStream` は `Finalize` + `Client.BatchCommitWriteStreams` が必要
- `BufferedStream` は `FlushRows(offset)` が必要

silently lossy なオプションを残すより拒否する方を選んだ。**サポートするなら `Flush` / `Close` にそれぞれの操作を組み込む必要がある。**

## `EnableWriteRetries` は渡していない

managedwriter 内部のリトライ（`defaultStreamSettings` では無効）は使わず、`Sinker` が `WithRetryPolicy` のポリシーで `gax.Invoke` する。**利用者が `WriterOptions` に `EnableWriteRetries(true)` を入れると二重にリトライされる。**

## `LoadJobs` の `FlushBytes` は BigQuery の制限対策ではない

SDK は `call.Media(...)` でアップロードし、`googleapi` の `ChunkSize` の godoc に「size bytes 以上のメディアは separate chunks でアップロードされる」とあるので、**リクエストサイズは SDK が分割する**。

`FlushBytes` が制御しているのは**バッファのメモリ使用量**。全行を flush まで保持するので、そこに上限が必要という理由。

BigQuery 側のリクエストサイズ上限とロードジョブ数 quota の具体的な数値は quota ページが取得できず**未確認**。`DefaultFlushRows = 10000` はジョブ数 quota を意識した定性的な値。

## 失敗時にバッファを捨てる

`flushLocked` は成功・失敗にかかわらず `buf.Reset()` する。**成功時だけクリアしていた時期があり、権限エラーなどで失敗し続けるとバッファが単調増加していた。** 行は失われるので、エラーメッセージに件数を含める。

`Migrate` の「失敗はキャッシュ、再 `New` で回復」という方針と揃えている。

## ロックを持ったままロードジョブを待つ

`Append` が閾値に達すると `flushLocked` がロードジョブの完了まで（秒〜分）ブロックし、その間ロックを保持する。**並行する `Sink` が全部待つ。** バッチ転送としては妥当だが、`Sinker` が "safe for concurrent use" と謳っているので GoDoc に明記してある。

## `Stager` を interface にした理由

`bqgcs` を別パッケージにすることで、**bqsink を import しただけでは `cloud.google.com/go/storage` がコンパイル対象に入らない**。GCS を使わない利用者に依存を負わせないため。

`Stager` が `Validator` を実装していれば `LoadJobs.Validate` から呼ばれるので、バケット未指定などは `New` の時点で弾ける。

`bqgcs.Staging` のオブジェクト名は `ナノ秒-連番.json`。並行する writer とリトライで衝突しないようにしている。
