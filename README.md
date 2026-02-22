# iidx_all_songs_master

Textage の `titletbl.js` / `datatbl.js` / `actbl.js` から IIDX 楽曲マスタ SQLite を生成し、`latest.json` と一緒に GitHub Releases へ公開するリポジトリです。

## Alias の正本

`music_title_alias` は以下 2 種類のみ生成します。

- `official`: `music.title` から収録スコープ別に自動生成
- `manual`: リポジトリ内 CSV から投入

外部 Wiki / Spreadsheet からの alias 取得は行いません。

### Manual CSV

- 既定パス: `data/music_alias_manual.csv`
- 設定キー: `settings.yaml` の `music_alias_manual_csv_path`
- 読み込みエンコーディング: `utf-8-sig`（UTF-8 BOM あり/なし両対応）

必須カラム:

- `textage_id`
- `alias`
- `alias_scope` (`ac` or `inf`)
- `alias_type` (`manual` 固定)

任意カラム:

- `note`

`title_canon` は不要です（存在しても読み込みでは使用しません）。

### バリデーション（失敗時ビルド停止）

- 必須カラム不足、必須値の空文字
- `alias_scope` が `ac/inf` 以外
- `alias_type` が `manual` 以外
- CSV 内 `(alias_scope, alias)` 重複
- `music` に存在しない `textage_id`（orphan）
- `UNIQUE(alias_scope, alias)` 衝突（official/manual 間も含む）

冗長行（`official` と同一の `textage_id + alias_scope + alias`）は警告を出してスキップします。

## DB 制約（alias）

`music_title_alias` の主な制約:

- `UNIQUE(alias_scope, alias)`
- `INDEX(textage_id)`
- `INDEX(alias_scope, alias)`
- `UNIQUE(textage_id, alias_scope, alias)`（重複防止）

## CI での必須検証

- `is_ac_active=1` 件数と `official/ac` 件数が一致
- `is_inf_active=1` 件数と `official/inf` 件数が一致
- `music_title_alias.textage_id` に orphan がない
- `alias_type` が `official/manual` のみ

## リリース成果物

- `song_master_<YYYY-MM-DD(.N)>.sqlite`
- `latest.json`

`latest.json` にはファイル名、`schema_version`、生成時刻、`sha256`、サイズ、Textage ソースハッシュを記録します。

## 運用メモ

- alias の変更は `data/music_alias_manual.csv` の差分として PR で追跡できます。
- 収録スコープ（`alias_scope`）により AC/INF の同名別曲を共存させられます。

