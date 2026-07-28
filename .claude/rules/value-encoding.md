---
paths:
  - "jsonrow.go"
  - "loadjobs_test.go"
---

# 値の表現（実測で確定させたもの）

ここの変換規則は**記憶や推測ではなく、実際に BigQuery に投げて確認したもの**。触るときは同じ方法で確認する。検証用の GCP プロジェクトにリテラルだけの readonly クエリを投げれば課金ゼロ。**プロジェクト ID はこのリポジトリに置かない。**

## `*big.Rat` を `encoding/json` に任せると壊れる

```
CAST('12.500000000' AS NUMERIC) → 12.5    ✓
CAST('25/2' AS NUMERIC)         → NULL    ✗
```

**`json.Marshal(big.NewRat(25,2))` は `"25/2"` を返す。** BigQuery はこれを読めず NULL になる。しかも `SAFE_CAST` なら NULL、通常の CAST ならエラーで、**黙ってデータが消える経路**だった。

`decimalString` が自前で変換している。整数なら `Num().String()` で全桁保持、小数なら `FloatString(scale)`。

## scale はスキーマから取る（Go の型からは決まらない）

`encodeJSONRow` がスキーマを受け取るのはこのため。**`*big.Rat` の適切な桁数は列の型と `Scale` で決まる。**

- `Scale` が設定されていればそれ
- NUMERIC なら 9（BigQuery の上限）
- BIGNUMERIC なら 38

Go の型だけで分岐していた時期があり、`FloatString(9)` を既定にしていたので **BIGNUMERIC 列が黙って 29 桁失われる**状態だった。

## 確認済みのテキスト表現

```
'2026-07-28T12:30:00Z'    → TIMESTAMP  ✓
'2026-07-28 12:30:00 UTC' → TIMESTAMP  ✓
'2026-07-28'              → DATE       ✓
'12:30:00'                → TIME       ✓
'2026-07-28T12:30:00'     → DATETIME   ✓
```

`civil.Date` / `civil.Time` / `civil.DateTime` の `String()` がそのまま使える。`time.Time` は RFC3339。

## JSON 列は転送層で表現が違う（実測で見つけたバグ）

**これはユニットテストが全部通っている状態で、実 BigQuery に書いて読み戻して初めて出た。**

| 転送層 | JSON 列に渡すもの |
|---|---|
| Load Jobs | **生の JSON 値**（`{"j":{"k":"v"}}`） |
| Storage Write | **JSON テキストの文字列**（`{"j":"{\"k\":\"v\"}"}`） |

ロードジョブに文字列として渡すと、BigQuery は**文字列そのものを JSON 値として格納する**。

```
before: "{\"url\":\"https://x/?a=1&b=2\"}"   ← 文字列が入ってしまった
after:  {"url":"https://x/?a=1&b=2"}          ← JSON 値として入る
```

`rowDialect` がこの差を吸収している。**この型は「TIMESTAMP の表現差」だけのために作ったものではない。** JSON 列も分岐するので、TIMESTAMP 用の名前（`timestampForm`）から改名した。

## `rowDialect` が分岐するのは2箇所だけ

| | Load Jobs | Storage Write |
|---|---|---|
| TIMESTAMP | RFC3339 テキスト | **int64 マイクロ秒** |
| JSON 列 | 生の JSON 値 | JSON テキストの文字列 |

**他の型は共通。** それは `storagewrite.go` が `WithProtoMapping` で NUMERIC / BIGNUMERIC / DATETIME / DATE / TIME を proto の string にマップしているから。詳細は `.claude/rules/transports.md`。

TIMESTAMP だけ共通にできないのは、BigQuery が TIMESTAMP の proto 型に string を許可していないため（int64 / int32 / uint32 / `google.protobuf.Timestamp` のみ）。

## REQUIRED 列の NULL を先に弾く理由

Storage Write では proto の `required` フィールドが未設定だと marshal 時に `proto: required field Row.req not set` で落ちる。**proto のフィールド名しか出ないので、どの列かを利用者が追いにくい。** `jsonValue` で列名付きのエラーにしている。
