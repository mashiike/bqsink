# bqsink

BigQuery への書き込みと、宣言スキーマへのテーブル追従（マイグレーション）を提供する Go ライブラリ。

使い方は README.md にある。ここには**コードを読んでも分からない前提**だけを置く。ファイル単位の罠・設計根拠は `.claude/rules/` にあり、対象ファイルを Read したときに自動ロードされる。

## このライブラリが存在する理由

BigQuery への書き込みコードを各所で書いていて、一番しんどいのが**スキーマ変更時の追従**だった。Go の構造体にフィールドを足したのに BQ 側の列追加を忘れて本番で落ちる、手で `ALTER TABLE` を打つ運用がデプロイ順序と噛み合わない、といった類。dbt の `on_schema_change` に相当するものをアプリ側の書き込み経路に持ち込むのが目的。

**したがって「宣言と実態のズレを機械的に検出する」ことが中核価値。** 黙って列を落とす・黙って食い違いを通す経路を作ると、このライブラリを使う動機そのものが消える。迷ったらエラーにする。

## 依存の向き（最重要）

**source of truth はコード側の宣言。BigQuery の実テーブルはそれに追従する側。** 逆ではない。

- `schemaUpdateOptions`（Load Job のスキーマ自動更新）は**使わない**
- Storage Write API の append レスポンスから updated schema を拾って writer を再構築する仕組みも**使わない**
- 届いたデータを見て列を発見する処理も**書かない**

これらはいずれも「テーブル側の変化を書き込み側が追いかける」機構で、向きが逆。スキーマ解決は `Declaration` の構築時（`DeclarationOf` / `DeclarationFromMetadata`）に1回で済み、書き込みの hot path に動的なスキーマ判定を持ち込まない。

## 決定済みで蒸し返さない設計判断

| 決定 | 理由 |
|---|---|
| `insertAll`（レガシーストリーミング挿入）は使わない | 公式に後継が案内されているレガシー API。2026 年の新規 OSS の主経路として筋が悪い |
| 転送層は Storage Write API と Load Jobs のみ | 上記の帰結 |
| struct は既定で JSON 列、RECORD は `record` タグ | ネストにフィールドを足してもマイグレーション不要になる。列プルーニングが効かなくなるトレードオフは受け入れ済み |
| 既定の戦略は `AppendNewColumns{CreateIfMissing: true}` | 追従が中核価値なので既定で有効。`AppendNewColumns` は非破壊（列追加と緩和のみ）なので事故の規模が小さい。DROP は `SyncAllColumns` を明示させる |
| Functional Options は `NewSinker` だけ | 転送層の設定は構造体リテラル + `NewWriter`（`net.Dialer` と同じ形）。1パッケージに2族の Option があると読む側がどちらか分からなくなる |
| **転送層の writer を先に作り、`NewSinker(w, decl, opts...)` に渡す**（2026-08-05） | 宛先・転送方式・バッファ・リトライは「書き込みに伴う内容」で writer に属し、宣言・和解・行の変換は「書き込み方に依らない処理」で Sinker に属する（`io.Writer` と `json.Encoder` の分割）。`os.File` が `Name()` を持つのと同じで、**宛先の同一性は writer 側にある** |
| `RowsWriter` は `Relation()` / `Client()` を**必須**メソッドにする | optional interface + 解決順序（strategy > writer > エラー）は複雑さを塗り広げるだけだった。本物の transport writer は必ず両方を持つので必須にしてもコストゼロ。`Client()` が nil は「BigQuery に繋がっていない」の表明で、`NewSinker` が `MigrationNone` 以外を拒否する |
| **テーブルの姿を宣言する Option は作らない**（`WithSchema` / `WithTableMetadata` は削除済み） | 行 struct にはドメイン知識が載っているので、列の意味を語れるのは行の型を定義した場所だけ。スキーマもテーブル設定もタグか `TableDefiner` に書く。他パッケージで定義された行を扱うことは想定しない。marshalers（値の符号化。他パッケージのフィールド型にはメソッドを足せないので外から登録する口が必要）は例外だが、渡し方は Option ではなく `DeclarationOf` / `DeclarationFromMetadata` の可変長引数（2026-08-06 に `WithMarshalers` Option を削除） |
| テーブルの description とラベルは埋め込み `TableMeta` のタグ | メソッドしか経路が無いのは他の宣言（列・パーティション・クラスタリング）と非対称だった。列を作らない埋め込みマーカーにタグを付ける形（`bun.BaseModel` と同じ）で埋めた |
| Option の名前は引数の型名に合わせる（`WithMigrationStrategy`） | godoc で隣に並ぶので、型名と揃っていないと目に付く。`WithWriteStrategy` は 2026-08-05 に削除（転送層は writer のコンストラクタで選ぶ） |
| GoDoc は英語 | `pkg.go.dev` に載る OSS。テストのサブテスト名も英語で統一 |
| メタデータ列は `_ingestion_` プレフィックス（`IngestionMetadata`） | 業務列と区別でき、列一覧で先頭に集まる。コード上の語彙と列名を揃えている。アンダースコア始まりが BQ で通ることは統合テストで実測済み |
| ログは `slog`、`WithLogger` 未指定なら `slog.DiscardHandler` で破棄 | ライブラリが利用者の `slog.Default()` に断りなく書き始めないため。`io.Discard` + TextHandler と違い `Enabled` が false なので整形コストも出ない |
| ログの `Error` レベルは使わない | エラーは戻り値で返している。ログにも出すと二重処理。`Warn` は「返らずに捨てている事象」専用で、ここが `_ = err` の代わり |
| `SinkerID` / `RowID` は UUID v7 | 辞書順が生成順になるのでクラスタリングキーに使える。v4 にすると失われる |
| **`io.Writer` の契約を写す。ただし `n` を返すのは `WriteResult.Wait`**（`WriteRows(ctx, rows) (WriteResult, error)` / `Wait(ctx) (n int, err error)`、`n < len(rows)` なら必ず non-nil error） | 「届いた数」を返す形にすると、届いていないのに成功を返す経路が作れない。失敗した行は `rows[n:]` で呼び出し側が持っているので損失報告 API が不要。**遅延ハンドルにしたのは 2026-08-05**: 同期→非同期は破壊的変更だが逆は糖衣で足せる / 両転送の素の形が遅延（`AppendResult` / `*bigquery.Job`）/ バッファ時も呼び出し単位で結末を報告できる。**2026-08-06 に二段 ack へ再設計**: `WriteResult` が何を約束するかは writer 次第で層相対——StorageWrite と `FlushRows` 未設定の LoadJobs は配送を約束するのに対し、`FlushRows` 設定時の LoadJobs だけバッファへの受理（入った事実）を約束し、配送は別途 `FlushRows` / `Close` が報告する。`LoadJobs.FlushRows > 0` では ack の単位が個々の `WriteRows` 呼び出しではなく flush（`FlushRows` / `Close`）のチェックポイントに変わり、`rows[n:]` を確認まで呼び出し側が保持する責任もそれに合わせて flush 単位になる |
| **`Sinker` はバッファリングしない。バッファは `LoadJobs.FlushRows` として転送層にある**（2026-08-05 に方針変更） | ジョブ数 quota は LoadJobs 固有の経済性で、転送に依らない性能アドオンではない。だから `StorageWrite` にバッファは無く `FlushRows` も `LoadJobsWriter` にしか生えていない。汎用 `BufferedWriter` デコレータ案と `Sinker.Flush` 案は却下。**2026-08-06 に共有バッチ会計（`loadBatch` / `claimed` / `unread` / `inflight` + `sync.Cond`）から二段 ack（`buf` + `pending` + `wg`）へ再設計。** **`FillRow` が `Sink` の中で走るので `_ingestion_at` は投入時刻にならない**（`Sinker` 側に溜めると壊れる） |
| `Sinker.SinkAsync(ctx, rows) (WriteResult, error)` を追加、`Sink` 自体は同期のまま（2026-08-06） | `Sink` と `SinkAsync` は共通処理（バッチ読み取り・型検査・初回の和解・`FillRow`・変換）を非公開の `send` に共有し、違いは `Wait` を呼ぶかどうかだけなので二重実装にならない。`Sink` = `send` + `Wait` + n の検査 |
| **リトライは行を持っている層が持つ** | 再送できるのは行を握っている層だけ。`Sinker` はマイグレーションだけリトライし、書き込みは `RowsWriter` の責務（`LoadJobs.RetryPolicy` / `StorageWrite` は managedwriter の `EnableWriteRetries`）。`Sinker` が `WriteRows` を包むと「writer が諦めた後にもう一度呼ぶ」形になり、欠損バグが再発する |
| `WithMigrationStrategy(strategy, retryPolicy)` は2引数 | `nil` = リトライなしを呼び出し側に必ず表明させるため。1引数だと 412 の自己修復が黙って無効になる状態を作れる。`MigrationRetryer` のような decorator 型にしなかったのは、`Plan` が純関数でリトライしたいのは `Metadata` / `Update` / DDL だから包めない |
| **`Sinker` はジェネリックにしない。行の型は `NewSinker` の `Declaration` で確定する**（2026-08-04 に `Sinker[T]` 廃止、2026-08-05 に確定を初回 `Sink` から構築時へ） | `New[T]` は T の**値**を一度も受け取らない（`tableMetadataOf` がゼロ値にメソッドを呼ぶ）ので、実行時に決まるスキーマを載せる経路が原理的に作れなかった。型を値で受ければ `reflect.StructOf` 由来の型も通る（**この経路は 2026-08-06 に `DeclarationForType` の削除とともに廃止**。実行時に決まるスキーマの経路は `DeclarationFromMetadata(md *bigquery.TableMetadata, ...)` に一本化された——行の型を `map[string]any` に固定し、スキーマは型の推論ではなく `md` というデータから読む）。先人はいずれも型を値で受けている（`dynamo.CreateTable(name string, from interface{})` / `bigquery.InferSchema(st interface{})` → `reflect.TypeOf(st)` / bun の `Model((*User)(nil))` / `cel-go` の `ext.NativeTypes`）。**2つ目の型が来たらエラー**にするのは、`relation` が1つなので複数の型が同じテーブルを宣言すると「宣言と実態のズレ」の比較対象が消えるため |
| `Sink` はスライスを受ける（`Sink(ctx, rows any)`。可変長引数にしない） | Go は `[]AccessLog` を `...any` に展開できない（実測済み）ので、可変長にするとバッチを渡すのに `[]any` への変換ループを全利用者が書くことになる。「渡された行がバッチ」が中核なのに主経路が重くなる。**スライス/配列はバッチ、それ以外は単一行**と読み替えて曖昧さが出ないのは、`buildRowPlanFor` が struct しか行にしないので行がスライスになる経路が存在しないため |
| **`Migrate` / `Schema` は公開しない** | 「和解したか」を利用者が気にする状態機械を公開契約にしない。`Sink` が必要なときに1回だけ和解する。**2026-08-05 に宣言が構築時に確定したので「型が決まる前は `Schema()` が nil」という当初の理由は消えたが、決定は維持する** — 公開すると `Migrate` の呼び忘れ・二重呼び出し・`Sink` との順序が契約になる。**2026-08-21 に `Sinker.Start(ctx) error` を例外として追加した**: `Migrate` そのもの（和解ロジックの分離呼び出し）や `Schema`（結果の問い合わせ）ではなく、`Sink` が初回にする和解を行を渡さず今すぐ起こすだけのトリガーで、二重呼び出しも `sync.Once` が吸収するので契約は増えない。追加理由は StorageWriter が `BindSchema` で渡された ctx を背景接続の生存期間に retain するため（`.claude/rules/migration.md` 参照）——常駐プロセスがリクエストスコープの短命 ctx で最初の `Sink` を呼ぶと、その ctx のキャンセルで以後の書き込みが全滅する。`Start` は呼び出し側にプロセス起動時の長命な ctx で和解させる経路を与える |
| **テーブルの姿を宣言する引数も作らない。`Declaration` が受けるのは*型*であって*宣言の中身*ではない** | Option を却下した理由（宣言は行の型に属する）は必須引数にもそのまま効く。`DeclarationOf[T]()` は T のタグとメソッドを読むだけで、外から内容を書く口はない。**`Declaration` に `Schema` フィールドを足したくなったら、それは削除した `WithSchema` の再来。** 当初は `map[string]any` を行にできないとしていた（名前なし型はメソッドを持てないので宣言の住所が無い）が、**2026-08-06 に `DeclarationFromMetadata(md *bigquery.TableMetadata, ...)` を明示的な例外として追加した**: 実行時にしか決まらないスキーマは型のタグに書けないので、`md` というデータを直接受ける。行の型は `map[string]any` に固定される |

## 実装の指針

- **`.claude/rules/` に逃がす**: godoc に書くほどではないが、コードから読めない罠・実測で判明した BigQuery の挙動・設計根拠。インラインコメントにはしない
- **行ローカルな安全マーカーだけインラインコメントにする**: 「この順序を変えると壊れる」類
- **BigQuery の挙動を記憶で断定しない**。ドキュメントで出なければ実測する。リテラルだけの readonly クエリなら課金ゼロで確かめられる。過去に記憶由来の誤りを出している（NULL ARRAY の扱い、BIGINT が別の型だという思い込み）
- **`WebFetch` の要約は行がまとめられていると情報が落ちる**。実際に「DATETIME と TIME が同じ行」だったせいで TIME の `int64` が要約から消え、`adapt` の実装と矛盾しているように見えた。型マッピングのような表を読むときは原文を要求する

## 行に列を足す仕組み

`RowFiller` は**ライブラリが型を押し付けない形**にしてある。埋め込んだ型のメソッドは promote されるので、`IngestionMetadata` を埋め込んでも自分の型を書いても同じように動く。

**`FillRow` は1行1回、変換とリトライの前に呼ぶ。** この契約があるので `AppendInfo` を薄く保てる（利用者が `FillRow` の中で `time.Now()` を呼んでもリトライでずれない）。順序を崩すと `_ingestion_row_id` が重複排除に使えなくなる。

## テスト

```
go test ./...                                    # ユニットのみ、ネットワークに出ない
BQSINK_TEST_PROJECT=... go test ./...            # + 統合テスト
BQSINK_TEST_PROJECT=... BQSINK_TEST_BUCKET=... go test ./...  # + GCS ステージング
```

**プロジェクト ID とバケット名をコードに書かない。** 環境変数が未設定なら Skip する。

ビルドタグは使っていない。環境変数だけで制御するので `go test ./...` でも常にコンパイル・型チェックが走る。

**ユニットテストを実ネットワークに出さない。** 転送層と DDL は内部 interface（`tableAPI` / `queryRunner` / `jobLoader` / `WriteStrategy`）で差し替える。実装を進めたときにテストが実際に GCP を叩き始めて 401 で落ちた事故がある。

**疎通は統合テストでしか取れない。** ユニットテストが全部通っている状態で、JSON 列の二重エンコードと `Close` の `io.EOF` という2つのバグが実 BigQuery で初めて出た。転送層に触ったら統合テストを回す。

## 未対応

- **ネスト RECORD 内部の変更のマイグレーション適用**。検出して `ErrSchemaConflict` を返すだけ。struct が既定で JSON なので `record` タグを使わない限り遭遇しない
- **`PendingStream` / `BufferedStream`**。commit / offset flush が必要で、やらないと行が永久に見えない。`Validate` で拒否している
- **exactly-once**。リトライで行が重複しうる
- **起動時ヘルスチェック（部分的に解消、2026-08-21）**。`NewSinker` も `NewWriter` も I/O しない点は変わらないが、`Sinker.Start(ctx)` を呼べば最初の実バッチより前に BigQuery に到達できる。呼ぶかどうかは呼び出し側の選択で、呼ばない利用者には依然として経路が無い（`Sink` は空スライスで早期 return するのでプローブにならない）
- **BigQuery のロードジョブ数 quota とリクエストサイズ上限の具体的な数値**。未確認。バッチサイズを決めるのは呼び出し側になったので、ライブラリ側に推測値の定数を置くのはやめた
