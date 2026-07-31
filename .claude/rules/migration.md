---
paths:
  - "migration.go"
  - "bqsink.go"
  - "relation.go"
  - "option.go"
  - "apply_test.go"
  - "bqsink_test.go"
  - "relation_test.go"
---

# マイグレーションと Sinker のライフサイクル

## `apply` の順序を変えてはいけない

**列追加 → 列削除の順。** 途中で失敗したら宣言より多い状態で止まる方が安全（データが消えていない）。

```go
if err := s.patchSchema(ctx, md, change); err != nil { return err }
if len(change.DropColumns) == 0 { return nil }
return s.dropColumns(ctx, change.DropColumns)
```

**drop があるときに early return する実装にしてはいけない。** DROP が未実装だった頃はそれが「中途半端な適用を防ぐ」正しい動作だったが、DROP が動くようになると**追加と削除の両方を含むプランで削除だけが適用されて成功を返す**。これは検出すべき divergence そのもの。`apply_test.go` の "adds and drops are both applied" がこれを守っている。

## `MigrationStrategy.Plan` は純関数（BigQuery を触らせない）

`Plan(state TableState, logger *slog.Logger) (SchemaChange, error)` は「差分のうち何を適用するか決める」だけ。**BigQuery 呼び出しを戦略に持たせない理由が2つある。**

1. `Metadata` 取得・etag の受け渡し・`Update` の発行・DDL の組み立てが戦略ごとに重複する
2. 戦略のテストが BigQuery 抜きで書ける。`IgnoreColumns` が効いているかの検証がまさにこれ

流れは `Metadata 取得 → DiffSchema → strategy.Plan → 適用` で、両端は `Sinker` の仕事。

**`logger` を渡しているのは「和解しなかった差分」を報告できる場所が他にないから。** `SchemaChange` は適用するものしか表現しないので、`MigrationNone` が差分を放置したこと・`SyncAllColumns` の `IgnoreColumns` がズレを隠したことは、戻り値からは消える。ズレの検出が中核価値なのでそこだけログにしている。**「純関数」は BigQuery を触らせないという意味で、ログは対象外。** `Plan` に ctx が無いので `WarnContext` ではなく `Warn` を使う。

## テーブル作成は `SchemaChange.CreateTable` で表現する

`Plan` に `TableState.Exists` を渡し、`SchemaChange.CreateTable` を返させている。**戦略に「テーブルを作るか」を問う2つ目のメソッドを足すのではなく、既存の1メソッドに載せた。**

既定の戦略は `AppendNewColumns{CreateIfMissing: true}` なので、**オプション未指定でテーブルが作られる**。追従がこのライブラリの中核価値であり、`AppendNewColumns` の変更は非破壊なのでこれを選んでいる。テーブルを触らせたくない場合は `MigrationNone{}` を明示する。

テーブルが存在せず `CreateTable` も要求されないときは `ErrTableMissing` を返す。書き込めないことが確定しているので早期に知らせる。

## `Migrate` は失敗もキャッシュする

`sync.Once` を使っているので、503 や IAM 権限の失敗は**プロセスが生きている間ずっと同じエラーを返す**。回復には再 `New` が必要。これは意図した設計。

**ctx は初回呼び出し側のものが使われる。** 後から別の ctx をキャンセルしても効かない。

`sync.Once` の代わりに mutex + 成功フラグにすると失敗からリトライできるが、失敗し続ける間 `Sink` ごとに `Metadata` を叩くことになる。前者を選んでいる。

## etag による楽観ロック

`table.Update(ctx, tm, etag)` に `md.ETag` を渡している。**複数レプリカが同時に同じ列を追加しようとする競合は、BigQuery が 412 を返すのでリトライで解決する。** 自前でロックを作る必要はない。

**リトライは `WithMigrationStrategy(strategy, retryPolicy)` の第2引数で決まる。** 2引数にしたのは、`nil` を渡す＝リトライしないことを呼び出し側に必ず表明させるため。1引数のままだと「412 の自己修復が黙って無効になっている」状態を作れてしまう。`WithMigrationStrategy` を呼ばなければ `New` が `AppendNewColumns{CreateIfMissing: true}` + `DefaultRetryPolicy` を入れるので、既定は安全側。

`retryPolicy` に `nil` を渡すと `retrying` が op を1回呼ぶだけになる（`gax.WithRetry(nil)` は使わない）。

リトライは毎回 `Metadata` から読み直す（`migrate` 全体をリトライしている）。etag だけ再取得しても意味がないため。

## `isRetryable` は両プロトコルを見る

ロードジョブは HTTP、Storage Write は gRPC でエラーを返すので、**片方だけ見ると転送層によってリトライされない。**

```
HTTP: 412, 409, 429, 500, 502, 503, 504
gRPC: Unavailable, DeadlineExceeded, ResourceExhausted, Internal, Aborted
```

`DefaultRetryPolicy` は `Migrate` の既定と `LoadJobs.RetryPolicy` の既定の両方で使われる。以前 `isConcurrentChange`（412/409 のみ）を使っていて、書き込みに転用したときリトライが効かなかった。**テストヘルパー `fastRetryPolicy` も同じ判定関数を使うこと**（バックオフだけ速い）。

`retrying` は `Sinker` のメソッドではなくパッケージレベル関数（`retrying(ctx, logger, what, newRetryer, op)`）。`Migrate` と `loadJobsWriter.WriteRows` の両方から呼ぶため。**書き込みのリトライを `Sinker` 側に戻さないこと** — 理由は `.claude/rules/transports.md`。

`attemptLimiter` は gax が意図的に提供しない回数制限を足すためのもの。godoc に "MaxNumRetries / RPCDeadline is specifically not provided" とある。

## `tableAPI` / `queryRunner` はテストのために存在する

`*bigquery.Table` と `*bigquery.Client` は具体型なのでモックできない。この2つの非公開 interface があるおかげで、**412 リトライ・conflict・テーブル作成・DDL の SQL 文字列**をネットワークなしで検証できる。

公開 API は変わらない。`Sinker.api` / `Sinker.query` にテストから fake を代入する（`newTestSinker` がやっている）。

## DROP は DDL なので列名を検証する

`ALTER TABLE` に列名を文字列で埋め込むため、`checkColumnName` で BigQuery が許す文字（英数字とアンダースコア）だけを通す。**テーブル名は `Relation.quoted()` でバッククォートで囲む。**

## `SinkerID` と `SinkerCreatedAt` は `New` で決まる

`New` の中で `newID()` と `time.Now()` を呼ぶ。**つまり `Sinker` インスタンスの生存期間を表す。**

- バッチごとに `New` する使い方 → `_ingestion_id` はバッチ単位
- 常駐プロセスが1つの `Sinker` を持ち続ける使い方 → プロセスが生きている間ずっと同じ ID

常駐プロセスで「実行単位」を表したいなら作り直す。外から ID を渡す `WithSinkerID` は**まだ用意していない**。必要になったら足す（それまでは自前の `RowFiller` で対応できる）。

`newID()` が失敗しうるので **`New` が UUID 生成のエラーを返す**。乱数源が読めないときだけ。

## `Relation` を独自型にした理由

`bigquery.Table` は `client.Dataset(...).Table(...)` を経由しないと API を呼べない（client 参照が unexported）。**構造体リテラルで作れるテーブル参照型が SDK に無い**ので、文字列 ↔ 参照の変換（`ParseRelation`）を置く場所として定義した。

`ProjectID` 省略時は `client.Project()` で埋める。`FullyQualifiedName()` は `projectID:datasetID.tableID`（コロン区切りのレガシー形式）なので、標準 SQL 表記と混同しない。

ドット3分割で安全なのは、SDK の godoc が dataset / table 名を「letters, numbers, underscores のみ」と明記しており、project ID にもドットが入らないため。

## スキーマを明示しても plan は必要

行を書くには struct を歩く必要があるので、`InferSchema` が失敗する型は `BigQueryTableMetadata` でスキーマを書き切っても `New` が失敗する。**「スキーマを明示すればタグ推論を完全に回避できる」わけではない。**

スキーマを外から渡す `WithSchema` / `WithTableMetadata` は削除済み（下記「宣言は行の型に属する」）。

`checkPlanAgainstSchema` が「struct が書く列が宣言スキーマに含まれるか」を検証する。含まれなければ I/O 前に失敗させる。逆（スキーマに余分な列がある）は許容し、その列は NULL のまま。

## 宣言は行の型に属する

**テーブルの姿を外から渡す Option は作らない。** `WithSchema` / `WithTableMetadata` は一度存在したが 2026-07-29 に削除した。

- スキーマ → struct タグ、またはタグで書けないもの（BIGNUMERIC の精度・列の policy tag・ネスト RECORD）は `TableDefiner.BigQueryTableMetadata()` の `Schema`
- テーブルの description とラベル → 埋め込んだ `TableMeta` のタグ、または同じメソッド
- パーティション・クラスタリング → 列のタグ、または同じメソッド
- 有効期限などその他 → メソッドのみ

**理由は「行 struct にはドメイン知識が載っている」こと。** 列の意味を知っているのは行の型を定義した場所なので、そこがテーブルの姿を語る唯一の場所であるべき。他パッケージで定義された行を扱うケースは想定していない（ドメイン知識を反映できないため）。

`WithMarshalers` は例外として残している。こちらは**テーブルの宣言ではなく値の符号化**で、`uuid.UUID` のような他パッケージのフィールド型には `FieldMarshaler` メソッドを足せないため外から登録する口が必要。

`resolveSchema(metadata, plan)` は「`BigQueryTableMetadata` に `Schema` があればそれ、無ければタグ導出」の2択だけになった。`config` にスキーマもメタデータも持っていない。

テストで「タグとメタデータの矛盾」を表で回すときは `New` ではなく `resolveTableMetadata` を直接呼ぶ。**メソッドは型ごとに1つしか書けないので、1つの型で複数の矛盾パターンを表現できない**（`layout_test.go`）。`New` 経由の end-to-end は1件だけ残してある。

## `TableMeta` は列を作らない埋め込みマーカー

テーブルの description とラベルだけメソッドしか経路が無かった非対称を埋めるために 2026-07-29 に追加した（`bun.BaseModel` と同じ形）。

- **列を作らないのは特別扱いではない。** exported フィールドが0個の埋め込み struct は `collectFields` が展開して何も残さないという既存の規則の結果（`sync.Mutex` と同じ）。だから `collectFields` には手を入れていない
- **タグは `tableMetaOf` が `reflect` で別に読む。** 列の走査とは独立
- **`t` の直下のフィールドしか見ない。** 埋め込みの奥にある `TableMeta` は読まない。「型を1目見ればテーブルが分かる」を保つため。緩めるのは後から非破壊でできる
- **`bqsink` / `partition` / `cluster` タグが付いていたらエラー。** 列を指すタグを黙って無視しない
- **`TableMeta` を2回埋め込むケースの検査は書いていない。** 同じ型を2つ埋め込むとフィールド名が衝突してコンパイルエラーになるので到達しない
- `labels:"k=v,k=v"` が曖昧にならないのは、**BigQuery のラベルのキーと値が小文字・数字・アンダースコア・ダッシュのみ**と公式ドキュメントに明記されているため（2026-07-29 に原文で確認）。`,` も `=` も入らないのでエスケープ不要。**文字種の検査は BigQuery に任せている**（国際文字も許されるので自前で狭めると誤って弾く）。`parseLabels` が見るのは並びの形だけ
