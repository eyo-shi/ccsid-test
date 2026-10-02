# Step 1 — CDC パイプライン 多言語 UTF-8 保持検証結果

- 実行日時: 2026-10-02T17:33:08+09:00
- ホスト: LK4C5V492J
- モード: verify
- DB2 コンテナ: —
- PutFile ディレクトリ: fixtures/putfile_samples
- PASS / FAIL / WAIT: 9 / 0 / 0

> ※ Docker DB2 (LUW) 上の UTF-8 文字列投入。5035→939 等の z/OS CCSID 変換は Step 3。

## スライド転記用テーブル

| # | カテゴリ | 検証内容 | 期待値 | 結果 | 備考 |
|---|---|---|---|---|---|
| REG-JPN | 多言語 UTF-8 保持 | JPN INSERT (op:c) | 日本語テスト / 混合文字列：日本語・English・123 | **PASS** | file=event_00.json |
| REG-SYZ | 多言語 UTF-8 保持 | SYZ INSERT (op:c) | 中文测试 / 简体中文混合测试 | **PASS** | file=event_01.json |
| REG-KOR | 多言語 UTF-8 保持 | KOR INSERT (op:c) | 한국어테스트 / 한글 혼합 테스트 | **PASS** | file=event_02.json |
| REG-DEU | 多言語 UTF-8 保持 | DEU INSERT (op:c) | Deutsch Test / Grüße aus München | **PASS** | file=event_03.json |
| REG-TUR | 多言語 UTF-8 保持 | TUR INSERT (op:c) | İstanbul / Türkçe karakter: ğüşıöç | **PASS** | file=event_04.json |
| REG-USA | 多言語 UTF-8 保持 | USA INSERT (op:c) | Hello World / English mixed 123 | **PASS** | file=event_05.json |
| DML-INS | CDC DML | INSERT (ID=9100) | INSERT確認 | **PASS** | op:c file=event_06.json |
| DML-UPD | CDC DML | UPDATE (ID=9100) | UPDATE確認 | **PASS** | op:u file=event_07.json |
| DML-DEL | CDC DML | 物理 DELETE (ID=9100) | before=中文测试, after=null | **PASS** | op:d file=event_08.json |

## 詳細

### REG-JPN — JPN INSERT (op:c)
- 期待: `日本語テスト / 混合文字列：日本語・English・123`
- 実際: `日本語テスト / 混合文字列：日本語・English・123`
- ステータス: **PASS**
- ソース: `fixtures/putfile_samples/event_00.json`
- 備考: file=event_00.json

### REG-SYZ — SYZ INSERT (op:c)
- 期待: `中文测试 / 简体中文混合测试`
- 実際: `中文测试 / 简体中文混合测试`
- ステータス: **PASS**
- ソース: `fixtures/putfile_samples/event_01.json`
- 備考: file=event_01.json

### REG-KOR — KOR INSERT (op:c)
- 期待: `한국어테스트 / 한글 혼합 테스트`
- 実際: `한국어테스트 / 한글 혼합 테스트`
- ステータス: **PASS**
- ソース: `fixtures/putfile_samples/event_02.json`
- 備考: file=event_02.json

### REG-DEU — DEU INSERT (op:c)
- 期待: `Deutsch Test / Grüße aus München`
- 実際: `Deutsch Test / Grüße aus München`
- ステータス: **PASS**
- ソース: `fixtures/putfile_samples/event_03.json`
- 備考: file=event_03.json

### REG-TUR — TUR INSERT (op:c)
- 期待: `İstanbul / Türkçe karakter: ğüşıöç`
- 実際: `İstanbul / Türkçe karakter: ğüşıöç`
- ステータス: **PASS**
- ソース: `fixtures/putfile_samples/event_04.json`
- 備考: file=event_04.json

### REG-USA — USA INSERT (op:c)
- 期待: `Hello World / English mixed 123`
- 実際: `Hello World / English mixed 123`
- ステータス: **PASS**
- ソース: `fixtures/putfile_samples/event_05.json`
- 備考: file=event_05.json

### DML-INS — INSERT (ID=9100)
- 期待: `INSERT確認`
- 実際: `INSERT確認`
- ステータス: **PASS**
- ソース: `fixtures/putfile_samples/event_06.json`
- 備考: op:c file=event_06.json

### DML-UPD — UPDATE (ID=9100)
- 期待: `UPDATE確認`
- 実際: `UPDATE確認`
- ステータス: **PASS**
- ソース: `fixtures/putfile_samples/event_07.json`
- 備考: op:u file=event_07.json

### DML-DEL — 物理 DELETE (ID=9100)
- 期待: `before=中文测试, after=null`
- 実際: `before='中文测试', after=None`
- ステータス: **PASS**
- ソース: `fixtures/putfile_samples/event_08.json`
- 備考: op:d file=event_08.json
