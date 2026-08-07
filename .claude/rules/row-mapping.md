---
paths:
  - "plan.go"
  - "fields.go"
  - "schema.go"
  - "marshal.go"
  - "maprow.go"
  - "plan_test.go"
  - "fields_test.go"
  - "schema_test.go"
  - "marshal_test.go"
  - "maprow_test.go"
---

# 型 → 列 / 値 → 行 の対応

## `rowPlan` が存在する理由（触る前に必ず読む）

**スキーマ導出と行変換で struct を2回歩くと必ず drift する。** `plan.go` の `rowPlan` は、その2つを1つの走査から作るために存在する。

- `buildRowPlan` が1回歩いて `[]fieldPlan` を作る
- `rowPlan.schema()` が宣言スキーマを返す
- `rowPlan.marshalRow()` が行を返す

**タグ解釈・ポインタの剥がし・`[]byte` の例外・REPEATED 判定・Marshaler の優先順位を2箇所に書いてはいけない。** 一度そうなっていて、片方が `isRepeated` を先に見て他方が Marshaler を先に見る、という非対称が生まれていた（偶然動いていた）。

`plan_test.go` の `TestRowKeysMatchTheSchema` がこの drift 全体を守っている。`everythingRow` は全パターンを1つの型に詰めていて、`marshalRow` のキー集合と宣言スキーマの列名が完全一致することを検査する。**フィールドの形を増やしたらこの型に足す。**

## 優先順位（変えると利用者の期待が壊れる）

`fieldPlan.resolveType` の分岐順序は仕様である。

1. `date` / `datetime` / `time` タグ（`time.Time` を TIMESTAMP 以外の列にする）
2. 登録された `Marshalers`（`DeclarationOf` / `DeclarationFromMetadata` の可変長引数）
3. 型自身の `FieldMarshaler`
4. well-known types（`time.Time`, `civil.*`, `big.Rat`）
5. `record` タグ（struct を RECORD にする）
6. プリミティブ
7. 構造を持つ型 → JSON

**2 が 3 より先なのは「呼び出し側が明示したものを最優先」という判断。** 型の持ち主でない人が外から上書きできることが `Marshalers` の存在理由なので、型自身の宣言に負けては意味がない。

**4 が 5 より先なので、`bqsink:",record"` を `time.Time` に付けても無視される。** well-known types はスカラー列で、降りていく RECORD ではないから。実測すると TIMESTAMP・ネストスキーマ0件になり、エラーにはならない。`date` / `datetime` / `time` との併用だけが `parseTag` でエラーになる（1 の導入で入れた検査）ので、この非対称はそこだけ表に出る。

## `date` / `datetime` / `time` が `time.Time` だけなのは意図

**1 を「変換が成分の切り捨てだけで定義できるペア」に限っている。** `civil.DateOf` / `DateTimeOf` / `TimeOf` はいずれも丸め方の選択がなく、失敗もしない。だから**このオプションは新しい実行時エラー経路を1本も作らない**。

除外したものと理由:

| 却下したペア | 理由 |
|---|---|
| `float64` → `int64` | 0方向への切り捨てと定義はできるが、NaN / Inf / 範囲外で Go の変換結果が**仕様上実装依存**。そこだけ実行時エラーという例外規則が要る。そして INTEGER 列が欲しいなら Go 側を `int64` と宣言すれば済む |
| `time.Time` → `int64`（epoch） | 秒 / ミリ / マイクロが曖昧 |
| `string` → `json` | 切り捨てでなく再解釈で、不正な JSON なら失敗する。`json.RawMessage` がある |
| `[]byte` → `string` | エンコーディングが曖昧 |

丸め方や失敗を伴う変換は `FieldMarshaler` / `MarshalFunc` の領分。**書く人が方針を明示する場所がある**ので、タグ側で暗黙に決めない。

### 独立タグキー（`data_type` / `type`）は却下済み。蒸し返さない

**列の型は `bqsink` タグのオプションで指定する。独立したタグキーは立てない。** 検討して却下した。

- `record` が同じ役割（Go の型から BQ の型を選ぶ）で `bqsink` タグにいる。同じ役割を2箇所に分けると、読む人が毎回「なぜ分かれているのか」を考える
- `partition` / `cluster` / `description` が独立キーなのは**テーブルの話で列の話ではない**から。この線引きで一貫する
- 対応するのが `time.Time` だけになったので、キーに名前を付ける必要自体が消えた（`data_type` は `INFORMATION_SCHEMA.COLUMNS.data_type` と同名にできる利点があったが、値が3つしかないなら見合わない）

**struct を `data_type:"record"` に移す案も却下済み。** `record` は `bqsink` タグに残す。

`big.Rat` を BIGNUMERIC 既定にするかは**これとは別の未決着の論点**で、ここでは決まっていない。混同しないこと。

## Marshaler と衝突したらエラーにする（precedence にしない）

`MarshalFunc[time.Time]` を登録した上で `bqsink:",date"` を書いたら初回 `Sink` で失敗する。どちらかを黙って勝たせない。

**エラー → precedence への緩和は後から非破壊でできるが、逆は利用者を壊す。** 迷ったので厳しい側から始めている。

`resolveTimeType` に `fieldMarshalerOf` の検査が無いのは意図的。メソッドは型自身のパッケージでしか定義できないので、利用者が `time.Time` に `FieldMarshaler` を実装させることは不可能で、到達しない分岐になる。

## `Marshalers` と `FieldMarshaler` は別物

`encoding/json/v2` の `Marshaler`（interface）と `Marshalers`（struct）の関係をそのまま踏襲している。名前が似ているだけで役割が違う。

- `FieldMarshaler` は**型が自分の代わりに**実装する。レシーバが値なので型情報が不要でメソッドは2つ
- `Marshalers` は**定義を変えられない型のために外から**登録する。`MarshalFunc[T]` の型パラメータが型情報を持つので、公開メソッドを持たない

`MarshalFunc` にポインタ型 `*T` を渡すと、`lookup` が `deref` するので永久に一致しない。だから `MarshalFunc` は `buildErr` に記録するだけで、**コンストラクタ自身はエラーを返せない**。報告するのは `joinMarshalers` で束ねた後、`buildRowPlan`（struct 経路）または `buildMapPlan`（map 経路）がその `err()` を確認して `Declaration.err` に載せる場所——つまり `DeclarationOf` / `DeclarationFromMetadata` を呼んだ時点。**依然として即座の戻り値ではないが、報告場所は以前の「`WithMarshalers` Option が評価されるまで遅延する」経路から、`Declaration.err` が `NewSinker` から転記されて返る経路に変わった。**

## 埋め込みは `encoding/json` の規則をそのまま実装している

`fields.go` の `collectFields` は勘で書いていない。実測して確認した挙動に合わせている。

```
promoted   {"ID":"i","Kind":"k","Name":"n"}      埋め込みが宣言位置に展開される
shadowed   {"Kind":"k","ID":"outer","Name":"n"}  同名は外側が勝ち、他は promote される
ambiguous  {"Kind":"k","Name":"n"}               同深度の衝突はその列だけ消える
deep       {"ID":"i","Kind":"k","Middle":"m","Name":"n"}
```

要点:

- **breadth-first で集めて、最後に index 順にソートする。** 列の順序は宣言順（埋め込みは展開位置）で、`slices.Compare` でそれを作っている。集めた順のままでは外側のフィールドが先に来て `encoding/json` と食い違う
- **浅い方が勝つ。同深度で衝突したらタグで名前を指定した方が勝つ。それでも決まらなければその列だけ消える**（他の列は promote されたまま）
- **unexported な埋め込み struct は展開する。** exported フィールドを持つ可能性があるため。unexported な非 struct の埋め込みは無視。これは `encoding/json` の実装と同じ分岐
- **タグで名前を付けた埋め込みは展開せず1列**になる
- 埋め込みポインタが nil のとき `FieldByIndex` は panic する。`marshalRow` は `FieldByIndexErr` を使ってその列を NULL にしている

`sync.Mutex` を埋め込んだ型が列を作らないのは、この展開の結果 exported フィールドが0個になるからで、特別扱いしているわけではない。

## `bigquery.InferSchema` と意図的に違う点

踏襲していると誤解しやすいので明記する。

| | `bigquery.InferSchema` | bqsink |
|---|---|---|
| 既定の mode | REQUIRED | **NULLABLE** |
| タグ | `bigquery:"..."` | `bqsink:"..."` |
| NULLABLE の指定 | `NullString` 等の SDK 型 | 既定なので不要 |
| struct | RECORD | **JSON**（`record` タグで RECORD） |
| map | エラー | **JSON**（string キーのみ） |
| `uint` / `uint64` | エラー | **NUMERIC** |

`uint` / `uint64` が NUMERIC なのは実測に基づく。`SAFE_CAST('18446744073709551615' AS INT64)` は NULL を返し、NUMERIC / BIGNUMERIC は保持する。**BIGINT は INT64 のエイリアスなので解決にならない**（`CAST(1 AS BIGINT)` の結果型が INTEGER であることを実測済み）。

## `nullifzero` の zero 判定

`encoding/json/v2` の `omitzero` に合わせて **`IsZero()` メソッドがあればそれを使う**。`reflect.Value.IsZero()` だけではゼロ値の `time.Time` を正しく扱えない。

repeated 列では意味が変わり「要素が0個」になる。nil と空 slice の両方が NULL。非 repeated とは判定関数が別（`isEmptySequence` と `isZeroValue`）。

`required` との併用は `parseTag` でエラーにする。REQUIRED 列は NULL を持てないので、宣言時に気づける方がよい。

## `FillRow` の呼び出し契約が `AppendInfo` を薄く保っている

`AppendInfo` に `Time` や `RowID` があるのは利便性のためで、**利用者が `FillRow` の中で `time.Now()` や `uuid.NewString()` を呼んでも同じ結果になる**。それが成り立つのは呼び出し契約のため。

```
Sink(ctx, rows) → FillRow(コピー) → marshalRow → retrying{ Append }
                       ↑1行1回                        ↑複数回ありうる
```

**この順序を崩してはいけない。** `FillRow` をリトライの内側に移すと `_ingestion_row_id` がリトライごとに変わり、重複排除に使えなくなる。`fill_test.go` の `TestRetryKeepsTheSameRowID` が守っている。

`AppendInfo` に試行回数を入れないのも同じ理由。

## 値レシーバの `FillRow` を `checkRowFiller` で弾く

コピーに対して呼ばれるので、書き換えたコピーが捨てられ、**列が空のまま黙って通る**。`checkRowFiller` がこれをエラーにしている。判定は「行の型（宣言がポインタなら `rt.Elem()`）へのポインタが `RowFiller` を実装している」かつ「行の型自身も実装している」= 値レシーバ。

**検査は `Declaration` の評価時点（`declarationOf`、`DeclarationOf` から呼ばれる）で走り、`Declaration.err` として `NewSinker` から返る** — 2026-08-05 までは初回 `Sink` だった。

**ポインタで宣言された行（`DeclarationOf[*T]()`）も同じ判定を受ける。これは 2026-08-06 に直したバグの結果で、以前は仕様であるかのように誤って書かれていた。** それまでの `checkRowFiller` は `rt.Kind() == reflect.Pointer` を早期 return で素通りさせ、値レシーバかどうかの検査を丸ごとバイパスしていた。`T` の `FillRow` が値レシーバでも `DeclarationOf[*T]()` と宣言すれば検査を逃れ、`_ingestion_*` などの列が空のまま黙って書かれる穴になっていた。旧版のこのファイルは「行がポインタとして渡される場合は検査せず、その場合だけ呼び出し側の値が書き換わる」とこの穴を仕様のように記述していたが誤りだった。直した後は `rt` がポインタなら先に `elem := rt.Elem()` へ剥がし、値レシーバかどうかの判定を常に `elem` に対して行う。`TestValueReceiverFillRowIsRejectedThroughAPointerDeclaration` がこの回帰を守っている。

ポインタ宣言でポインタレシーバの `FillRow` を実装している通常のケース（`*AccessLog` が `*T` として `RowFiller` を実装する場合）はこの検査を素直に通り、`fillable` が呼び出し側のポインタをそのまま返すので**呼び出し側の値まで書き換わる**（`RowFiller` の GoDoc に明記済み、`TestPointerTypeParameterCanFill` が守る）。

`reflect.StructOf` 経路（`DeclarationForType`）は 2026-08-06 に廃止され、実行時スキーマは `DeclarationFromMetadata`（`map[string]any` 行、データ駆動）に一本化された。これにより `checkUnpromotedFiller` が対処していた問題の発生源が消え、関数自体も削除された。

**コピーを作るのは `fillable`**（`bqsink.go`）で、値なら `reflect.New(v.Type())` + `Elem().Set(v)`、ポインタならそのポインタを返す。`prepare` はその戻り値に対して `RowFiller` をアサートするので、アクセサを組み立てる関数は存在しない（`fillerFunc` は 2026-08-04 に削除した）。

## `map[string]any` 行の値ディスパッチ順（`maprow.go`）

`DeclarationFromMetadata` は行を `map[string]any` に固定し、スキーマを `*bigquery.TableMetadata` から読む。`rowPlan` と違って歩く Go struct が無いので、`mapPlan` は列を Go の型からではなく `bigquery.Schema` から作る。**`RowFiller` はここでは効かない** — `map[string]any` は名前を持たないのでメソッドを実装できず、`DeclarationFromMetadata` は `fills` を立てない（`checkRowFiller` もそもそも呼ばれない）。行に自前の ingestion 列を持たせたい場合は `Sink` の前に呼び出し側が直接 map へ詰める。

`mapPlan.marshalScalar` が1つの非 nil 値をディスパッチする順序は仕様である:

1. 登録された `Marshalers`（値の**動的型**で引く。struct 経路の `fieldPlan.resolveType` は静的なフィールド型で引くのに対し、map の値は `any` なので動的型しか引ける手がかりがない）
2. 値自身の `FieldMarshaler`
3. 受理表（`reflect.Kind` ベース）。struct 経路と同じく named type（`type Env string` 等）も受理するが、`time.Time` / `civil.Date` / `civil.Time` / `civil.DateTime` / `*big.Rat` / `map[string]any` は型アサーションで厳密な型のまま受理する

**1・2 の変換結果が宣言列の `FieldType` と食い違ったらエラーにする。** struct 経路の既存エンコーダには無い検査で、map 経路で新設した。schema が run time に確定するので、`Marshalers` や `FieldMarshaler` の宣言と食い違ったまま書き込む経路を作らないため。

**REPEATED 列の要素が nil だとエラーにする。列自体が nil なら NULL のまま。** BigQuery は要素に NULL を持つ配列の書き込みを "Array cannot have a null element" として拒否することを実測で確認済み（クエリの中間値としては `ARRAY_LENGTH` がその NULL 要素も数えるが、それとは非対称）。列自体の nil 判定（`marshalField`）と要素の nil 判定（`marshalRepeated`）が別の関数になっているのはこの区別のため。

詳細（`marshalAccepted` の型ごとの分岐、`marshalInteger` の範囲チェック等）は `maprow.go` を読むこと。

## `_ingestion_` プレフィックスは実測で通ることを確認済み

列名は `_ingestion_at` / `_ingestion_id` / `_ingestion_row_id`（`IngestionMetadata`）。BigQuery の列名はアンダースコアで始められる（`FieldSchema.Name` の godoc に "must start with a letter or underscore"）。`_PARTITIONTIME` などの疑似列は大文字なので衝突しない。

**統合テスト `TestIntegrationIngestionMetadata` が実際にテーブルを作って確認している。** 列名の規則を変えるときはこれを回す。

`_sink_` / `_sinker_` は検討途中の名前で**採用していない**。`_sink_id` は `Sink` を呼ぶたびに変わる印象になり、`_sinker_id` は型名の露出になるため、コード上の語彙（`IngestionMetadata`）と揃えて `_ingestion_` にした。

## UUID v7 を選んだ理由

`SinkerID` と `RowID` は `uuid.NewV7()`。**v7 は Unix ミリ秒がプレフィックスなので、文字列として辞書順ソートすると生成順になる。** BQ のクラスタリングキーに使えるのはこの性質のため。v4 に変えるとこれが失われる。

`fill_test.go` の `TestRowIDsSortByTime` が単調増加を検査している。

`NewV7()` は `(UUID, error)` を返すので、`NewSinker`（`SinkerID`）と `prepare`（`RowID`）がエラーを返す形になっている。
