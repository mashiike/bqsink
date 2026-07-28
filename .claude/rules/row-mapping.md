---
paths:
  - "plan.go"
  - "fields.go"
  - "schema.go"
  - "marshal.go"
  - "plan_test.go"
  - "fields_test.go"
  - "schema_test.go"
  - "marshal_test.go"
---

# 型 → 列 / 値 → 行 の対応

## `rowPlan` が存在する理由（触る前に必ず読む）

**スキーマ導出と行変換で struct を2回歩くと必ず drift する。** `plan.go` の `rowPlan` は、その2つを1つの走査から作るために存在する。

- `buildRowPlan` が1回歩いて `[]fieldPlan` を作る
- `rowPlan.schema()` が宣言スキーマを返す
- `rowPlan.marshalRow()` が行を返す

**タグ解釈・ポインタの剥がし・`[]byte` の例外・REPEATED 判定・Marshaler の優先順位を2箇所に書いてはいけない。** 一度そうなっていて、片方が `isRepeated` を先に見て他方が Marshaler を先に見る、という非対称が生まれていた（偶然動いていた）。

`plan_test.go` の `TestRowKeysMatchTheSchema` がこの drift 全体を守っている。`everythingRow` は全パターンを1つの型に詰めていて、`toRow` のキー集合と宣言スキーマの列名が完全一致することを検査する。**フィールドの形を増やしたらこの型に足す。**

## 優先順位（変えると利用者の期待が壊れる）

`fieldPlan.resolveType` の分岐順序は仕様である。

1. `date` / `datetime` / `time` タグ（`time.Time` を TIMESTAMP 以外の列にする）
2. 登録された `Marshalers`（`WithMarshalers`）
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

`MarshalFunc[time.Time]` を登録した上で `bqsink:",date"` を書いたら `New` で失敗する。どちらかを黙って勝たせない。

**エラー → precedence への緩和は後から非破壊でできるが、逆は利用者を壊す。** 迷ったので厳しい側から始めている。

`resolveTimeType` に `fieldMarshalerOf` の検査が無いのは意図的。メソッドは型自身のパッケージでしか定義できないので、利用者が `time.Time` に `FieldMarshaler` を実装させることは不可能で、到達しない分岐になる。

## `Marshalers` と `FieldMarshaler` は別物

`encoding/json/v2` の `Marshaler`（interface）と `Marshalers`（struct）の関係をそのまま踏襲している。名前が似ているだけで役割が違う。

- `FieldMarshaler` は**型が自分の代わりに**実装する。レシーバが値なので型情報が不要でメソッドは2つ
- `Marshalers` は**定義を変えられない型のために外から**登録する。`MarshalFunc[T]` の型パラメータが型情報を持つので、公開メソッドを持たない

`MarshalFunc` にポインタ型 `*T` を渡すと、`lookup` が `deref` するので永久に一致しない。だから `MarshalFunc` は `buildErr` に記録して `WithMarshalers` / `InferSchema` で報告する。**コンストラクタがエラーを返せないので遅延報告になっている。**

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
Sink(ctx, v) → FillRow(コピー) → toRow → retrying{ Append }
                    ↑1行1回                     ↑複数回ありうる
```

**この順序を崩してはいけない。** `FillRow` をリトライの内側に移すと `_ingestion_row_id` がリトライごとに変わり、重複排除に使えなくなる。`fill_test.go` の `TestRetryKeepsTheSameRowID` が守っている。

`AppendInfo` に試行回数を入れないのも同じ理由。

## 値レシーバの `FillRow` は `New` で弾く

`FillRow` はコピーに対して呼ばれるので、値レシーバだと**書き換えたコピーが捨てられて列が空のまま黙って通る**。`rowFillerOf` が `reflect` で検出してエラーにしている。

判定は「`*T` が実装している」かつ「`T`（値）も実装している」= 値レシーバ。`T` がポインタ型の場合は別経路で、その場合だけ**呼び出し側の値が書き換わる**（GoDoc に明記済み）。

## `_ingestion_` プレフィックスは実測で通ることを確認済み

列名は `_ingestion_at` / `_ingestion_id` / `_ingestion_row_id`（`IngestionMetadata`）。BigQuery の列名はアンダースコアで始められる（`FieldSchema.Name` の godoc に "must start with a letter or underscore"）。`_PARTITIONTIME` などの疑似列は大文字なので衝突しない。

**統合テスト `TestIntegrationIngestionMetadata` が実際にテーブルを作って確認している。** 列名の規則を変えるときはこれを回す。

`_sink_` / `_sinker_` は検討途中の名前で**採用していない**。`_sink_id` は `Sink` を呼ぶたびに変わる印象になり、`_sinker_id` は型名の露出になるため、コード上の語彙（`IngestionMetadata`）と揃えて `_ingestion_` にした。

## UUID v7 を選んだ理由

`SinkerID` と `RowID` は `uuid.NewV7()`。**v7 は Unix ミリ秒がプレフィックスなので、文字列として辞書順ソートすると生成順になる。** BQ のクラスタリングキーに使えるのはこの性質のため。v4 に変えるとこれが失われる。

`fill_test.go` の `TestRowIDsSortByTime` が単調増加を検査している。

`NewV7()` は `(UUID, error)` を返すので、`New` と `appendInfo` がエラーを返す形になっている。
