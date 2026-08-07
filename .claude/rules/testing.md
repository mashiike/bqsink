---
paths:
  - "integration_test.go"
  - "*_test.go"
---

# テストの方針

## ユニットテストを実ネットワークに出さない

**実装を進めたときにテストが実際に GCP を叩き始めて 401 / PermissionDenied で落ちた事故がある。** `queryRunner` と `StorageWrite.Open` を実装した瞬間に3つのテストが実接続に変わった。

差し替え口:

| 対象 | interface | fake |
|---|---|---|
| `table.Metadata` / `Create` / `Update` | `tableAPI` | `fakeTable` |
| DDL（`client.Query`） | `queryRunner` | `fakeQueryRunner` |
| ロードジョブ | `jobLoader` | `fakeLoader` |
| 転送層全体 | `RowsWriter` / `WriteResult` | `fakeWriter`（`newFakeWriter(t)`） |
| GCS ステージング | `Stager` | `fakeStager` |

ヘルパーは3つ。**`newTestSinker[T](t, fake *fakeTable, w RowsWriter, opts ...Option)`** が `api` と `query` に fake を挿し、**`w` に nil を渡すと `newFakeWriter(t)` を既定で入れる**。実 writer を渡すと GCP を叩くので、`Sink` を呼ぶテストは必ず fake を通す。**`testSinker[T](t, opts ...Option) (*Sinker, error)`** は宣言の解決だけを見るテスト用（fake writer 上に `NewSinker` するだけ。旧 `settledSinker` の後継）。

**宣言の解決が `NewSinker` に移った**ので、タグの誤り・宣言スキーマとの不一致・値レシーバの `FillRow` は**構築時のエラー**になる。以前は初回 `Sink` まで出なかった。`ErrTableMissing` のように I/O が要る検査だけが `Sink`（`start`）の側に残る。

`fakeWriter.client` を nil にすると「BigQuery に繋がっていない writer」を作れる。**`NewSinker` は `MigrationNone` 以外の戦略をそのとき拒否する**ので、その検査のために使う。

`bigquery.NewClient(ctx, "test-project", option.WithoutAuthentication())` で認証なしの client が作れる。`NewSinker` も `NewWriter` も I/O しない設計なので、構築部分はこれでテストできる。

## 疎通は統合テストでしか取れない

**ユニットテストが全部通っている状態で、実 BigQuery に書いて初めて2つのバグが出た。**

1. JSON 列の二重エンコード（文字列として格納されていた）
2. `ManagedStream.Close()` の `io.EOF` を失敗として扱っていた

**転送層・値の表現・DDL に触ったら統合テストを回す。** ユニットテストの緑は疎通の保証ではない。

**SKIP 件数を数えること。** `go test` の `ok` は SKIP を含むので、末尾3行だけ見ると全件通ったように見える。実際に「`storageWriteWriter.Flush` / `Close` の新しい分岐をどのテストも通っていない」のを見落として完了報告を出しかけた（2026-07-29）。

```
go test -v ./... | grep -c '^--- SKIP'
```

環境変数が揃わず回せない状況で転送層を触ったら、**完了報告の「検証したこと」に未検証範囲として明記し、マージ前の実行を依頼する。** 黙って「テスト通過」と書かない。

## 環境変数（コードに ID を書かない）

```
go test ./...                                    # ユニットのみ
BQSINK_TEST_PROJECT=... go test ./...            # + 統合
BQSINK_TEST_PROJECT=... BQSINK_TEST_BUCKET=... go test ./...  # + GCS
```

| 変数 | 未設定時 |
|---|---|
| `BQSINK_TEST_PROJECT` | 全統合テストが Skip |
| `BQSINK_TEST_DATASET` | `bqsink_integration_test` を使う（無ければ作る） |
| `BQSINK_TEST_BUCKET` | GCS ステージングの3件だけ Skip |

**プロジェクト ID もバケット名もソースに入れない。** 環境変数だけで受け取り、未設定なら Skip する。汎用の `GOOGLE_CLOUD_PROJECT` を使わないのは、他のツール用に設定されているだけで意図せずテーブルが作られて課金されるのを避けるため。

## ビルドタグを使わない

環境変数だけで制御しているので、`go test ./...` でも統合テストが**コンパイル・型チェックされる**。ビルドタグにすると型エラーに気づけなくなる。

## 統合テストの後片付け

- テーブル名は `テスト名_ナノ秒`。並行実行で衝突しない
- `t.Cleanup` でテーブルを削除する。**データセットは残す**（他のテストも使う）
- GCS のオブジェクトは `Stager` の cleanup が消す。`Keep: true` のテストは自分で消す
- `readRows` は書き込みの伝播を待つためにリトライする（最大 90 秒）

## テストの期待値を実測に合わせる（逆をやらない）

このセッションで**テストの期待値が間違っていたケースが3回**ある。

- nil slice が NULL になると期待した（BigQuery の REPEATED は空配列。ただし NULL も保持できることを後に実測）
- `time.UnixMicro()` の値を手計算で間違えた
- `` `&` `` を raw string で書いて `&` を検出したつもりになっていた（`&` そのものを検索していた）

**実装とテストが食い違ったとき、まず実測でどちらが正しいかを決める。** リテラルだけの readonly クエリなら課金ゼロ。

## 修正が効いていることを確かめる

バグを直したら、**直前の実装で落ちることを確認する**。ネスト RECORD の検出を入れたときは判定を一時的に無効化して3ケースが落ちるのを見た。テストが検出能力を持っているかは、通ることでは分からない。
