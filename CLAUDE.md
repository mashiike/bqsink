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

これらはいずれも「テーブル側の変化を書き込み側が追いかける」機構で、向きが逆。スキーマ解決は起動時（初回 `Sink` 時）に1回で済み、書き込みの hot path に動的なスキーマ判定を持ち込まない。

## 決定済みで蒸し返さない設計判断

| 決定 | 理由 |
|---|---|
| `insertAll`（レガシーストリーミング挿入）は使わない | 公式に後継が案内されているレガシー API。2026 年の新規 OSS の主経路として筋が悪い |
| 転送層は Storage Write API と Load Jobs のみ | 上記の帰結 |
| struct は既定で JSON 列、RECORD は `record` タグ | ネストにフィールドを足してもマイグレーション不要になる。列プルーニングが効かなくなるトレードオフは受け入れ済み |
| 既定の戦略は `AppendNewColumns{CreateIfMissing: true}` | 追従が中核価値なので既定で有効。`AppendNewColumns` は非破壊（列追加と緩和のみ）なので事故の規模が小さい。DROP は `SyncAllColumns` を明示させる |
| Functional Options は `New` だけ | Strategy の設定は構造体リテラル。`New` の Option とどちらか分からなくなるのを避ける |
| **テーブルの姿を宣言する Option は作らない**（`WithSchema` / `WithTableMetadata` は削除済み） | 行 struct にはドメイン知識が載っているので、列の意味を語れるのは行の型を定義した場所だけ。スキーマもテーブル設定もタグか `TableDefiner` に書く。他パッケージで定義された行を扱うことは想定しない。`WithMarshalers` だけは例外（テーブルの宣言ではなく値の符号化で、他パッケージのフィールド型にはメソッドを足せない） |
| テーブルの description とラベルは埋め込み `TableMeta` のタグ | メソッドしか経路が無いのは他の宣言（列・パーティション・クラスタリング）と非対称だった。列を作らない埋め込みマーカーにタグを付ける形（`bun.BaseModel` と同じ）で埋めた |
| Option の名前は引数の型名に合わせる（`WithMigrationStrategy` / `WithWriteStrategy`） | godoc で隣に並ぶので、片方だけ `Strategy` が落ちていると目に付く |
| GoDoc は英語 | `pkg.go.dev` に載る OSS。テストのサブテスト名も英語で統一 |
| メタデータ列は `_ingestion_` プレフィックス（`IngestionMetadata`） | 業務列と区別でき、列一覧で先頭に集まる。コード上の語彙と列名を揃えている。アンダースコア始まりが BQ で通ることは統合テストで実測済み |
| ログは `slog`、`WithLogger` 未指定なら `slog.DiscardHandler` で破棄 | ライブラリが利用者の `slog.Default()` に断りなく書き始めないため。`io.Discard` + TextHandler と違い `Enabled` が false なので整形コストも出ない |
| ログの `Error` レベルは使わない | エラーは戻り値で返している。ログにも出すと二重処理。`Warn` は「返らずに捨てている事象」専用で、ここが `_ = err` の代わり |
| `SinkerID` / `RowID` は UUID v7 | 辞書順が生成順になるのでクラスタリングキーに使える。v4 にすると失われる |
| **`RowWriter` は `io.Writer` の契約を写す**（`WriteRows(ctx, rows) (n int, err error)`、`n < len(rows)` なら必ず non-nil error） | 「届いた数」を返す形にすると、届いていないのに成功を返す経路が作れない。失敗した行は `rows[n:]` で呼び出し側が持っているので、識別子を持ち回る損失報告 API（`UndeliveredError` 等）が不要になる |
| **`Sinker` はバッファリングしない。`Sink(ctx, vs...)` に渡された行がバッチ** | `io.Writer` にバッファが無く `bufio` が別パッケージなのと同じ配置。溜める層が「失敗した行を捨てるか保持するか」を持つと欠損の温床になる。追跡ポリシーは呼び出し側のもの |
| **リトライは行を持っている層が持つ** | 再送できるのは行を握っている層だけ。`Sinker` は `Migrate` だけリトライし、書き込みは `RowWriter` の責務（`LoadJobs.RetryPolicy` / `StorageWrite` は managedwriter の `EnableWriteRetries`）。`Sinker` が `WriteRows` を包むと「writer が諦めた後にもう一度呼ぶ」形になり、欠損バグが再発する |
| `WithMigrationStrategy(strategy, retryPolicy)` は2引数 | `nil` = リトライなしを呼び出し側に必ず表明させるため。1引数だと 412 の自己修復が黙って無効になる状態を作れる。`MigrationRetryer` のような decorator 型にしなかったのは、`Plan` が純関数でリトライしたいのは `Metadata` / `Update` / DDL だから包めない |

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
- **バッファリング**。`Sinker` は溜めない。`Sink(ctx, vs...)` に渡された行がそのままバッチで、溜めるのは呼び出し側の仕事。`bufio.NewWriter` 相当で `Sinker` を外から包む `BufferedSinker` を足す余地はある（罠は `.claude/rules/transports.md`）
- **BigQuery のロードジョブ数 quota とリクエストサイズ上限の具体的な数値**。未確認。バッチサイズを決めるのは呼び出し側になったので、ライブラリ側に推測値の定数を置くのはやめた
