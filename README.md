# iidx_all_songs_master

Textage の `titletbl.js` / `datatbl.js` / `actbl.js` を取り込み、IIDX 全曲マスター SQLite を生成するリポジトリです。  
このプロジェクトで最も重要なのは、配布・参照される生成物（SQLite と `latest.json`）です。

## 生成物（最重要）

### 生成物一覧

| 生成物 | 例 | 役割 | 生成元 |
| --- | --- | --- | --- |
| バージョン付き SQLite | `song_master_2026-02-22.sqlite` | 配布対象の本体 DB | `main.py` |
| マニフェスト | `latest.json` | 最新 DB の同定情報（ファイル名/ハッシュ/サイズ/生成時刻） | `main.py` |

### SQLite ファイル命名規則

| 項目 | 内容 |
| --- | --- |
| 元設定 | `settings.yaml` の `output_db_path` |
| 変換規則 | `output_db_path` の stem + `_<UTC日付>.sqlite` |
| 例1 | `song_master.sqlite` -> `song_master_2026-02-22.sqlite` |
| 例2 | `out/song_master.sqlite` -> `out/song_master_2026-02-22.sqlite` |

### DB テーブル一覧

| テーブル | 主用途 | 主キー/一意制約 | 備考 |
| --- | --- | --- | --- |
| `music` | 曲単位のマスター情報 | `music_id` (PK), `textage_id` (UNIQUE) | `title_search_key`, `title_qualifier` を保持 |
| `chart` | 譜面単位の情報 | `chart_id` (PK), `(music_id, play_style, difficulty)` (UNIQUE) | SP/DP 各難易度のレベル・ノーツ・全体/AC/INF 有効フラグ |
| `music_title_alias` | タイトル同定用エイリアス | `alias_id` (PK), `(alias_scope, alias)` (UNIQUE), `(textage_id, alias_scope, alias)` (UNIQUE) | `official` / `manual` を保持 |
| `meta` | 生成メタデータ | なし（単一最新行運用） | `schema_version`, `asset_updated_at`, `generated_at` |

### `music` テーブル定義

| カラム | 型 | NULL | 制約/既定値 | 説明 |
| --- | --- | --- | --- | --- |
| `music_id` | `INTEGER` | NOT NULL | `PRIMARY KEY AUTOINCREMENT` | 内部 ID |
| `textage_id` | `TEXT` | NOT NULL | `UNIQUE` | Textage の安定キー |
| `version` | `TEXT` | NOT NULL |  | 収録バージョン |
| `title` | `TEXT` | NOT NULL |  | 曲名 |
| `title_qualifier` | `TEXT` | NOT NULL | `DEFAULT ''` | 表示用修飾子（例: `(AC)`） |
| `title_search_key` | `TEXT` | NOT NULL |  | 検索用正規化キー |
| `artist` | `TEXT` | NOT NULL |  | アーティスト |
| `genre` | `TEXT` | NOT NULL |  | ジャンル |
| `is_ac_active` | `INTEGER` | NOT NULL |  | AC 収録フラグ（0/1） |
| `is_inf_active` | `INTEGER` | NOT NULL |  | INFINITAS 収録フラグ（0/1） |
| `last_seen_at` | `TEXT` | NOT NULL |  | 最終確認時刻 |
| `created_at` | `TEXT` | NOT NULL |  | 作成時刻 |
| `updated_at` | `TEXT` | NOT NULL |  | 更新時刻 |

### `chart` テーブル定義

| カラム | 型 | NULL | 制約/既定値 | 説明 |
| --- | --- | --- | --- | --- |
| `chart_id` | `INTEGER` | NOT NULL | `PRIMARY KEY AUTOINCREMENT` | 譜面 ID |
| `music_id` | `INTEGER` | NOT NULL | `FOREIGN KEY -> music(music_id)` | 親曲 ID |
| `play_style` | `TEXT` | NOT NULL |  | `SP` / `DP` |
| `difficulty` | `TEXT` | NOT NULL |  | 難易度 |
| `level` | `INTEGER` | NOT NULL |  | レベル |
| `notes` | `INTEGER` | NOT NULL |  | ノーツ数 |
| `is_active` | `INTEGER` | NOT NULL |  | 全体有効フラグ（レベル>0 の譜面存在フラグ）（0/1） |
| `is_ac_active` | `INTEGER` | NOT NULL |  | AC 収録有効フラグ（0/1） |
| `is_inf_active` | `INTEGER` | NOT NULL |  | INFINITAS 収録有効フラグ（0/1） |
| `last_seen_at` | `TEXT` | NOT NULL |  | 最終確認時刻 |
| `created_at` | `TEXT` | NOT NULL |  | 作成時刻 |
| `updated_at` | `TEXT` | NOT NULL |  | 更新時刻 |

### `music_title_alias` テーブル定義

| カラム | 型 | NULL | 制約/既定値 | 説明 |
| --- | --- | --- | --- | --- |
| `alias_id` | `INTEGER` | NOT NULL | `PRIMARY KEY AUTOINCREMENT` | エイリアス ID |
| `textage_id` | `TEXT` | NOT NULL | `FOREIGN KEY -> music(textage_id)` | 紐づく曲 ID |
| `alias_scope` | `TEXT` | NOT NULL | `CHECK(alias_scope IN ('ac','inf'))` | スコープ |
| `alias` | `TEXT` | NOT NULL |  | 同定対象文字列 |
| `alias_type` | `TEXT` | NOT NULL | `CHECK(alias_type IN ('official','manual'))` | エイリアス種別 |
| `created_at` | `TEXT` | NOT NULL |  | 作成時刻 |
| `updated_at` | `TEXT` | NOT NULL |  | 更新時刻 |

### `meta` テーブル定義

| カラム | 型 | NULL | 説明 |
| --- | --- | --- | --- |
| `schema_version` | `TEXT` | NOT NULL | スキーマバージョン |
| `asset_updated_at` | `TEXT` | NOT NULL | 参照元アセット更新時刻 |
| `generated_at` | `TEXT` | NOT NULL | 生成時刻 |

### 主要インデックス/一意制約

| 名称 | 対象 | 種別 | カラム |
| --- | --- | --- | --- |
| `idx_chart_music_active` | `chart` | INDEX | `(music_id, is_active)` |
| `idx_chart_filter` | `chart` | INDEX | `(play_style, difficulty, level, is_active)` |
| `idx_chart_notes_active` | `chart` | INDEX | `(is_active, notes)` |
| `idx_music_title_search_key` | `music` | INDEX | `(title_search_key)` |
| `idx_music_title_alias_textage_id` | `music_title_alias` | INDEX | `(textage_id)` |
| `uq_music_title_alias_scope_alias` | `music_title_alias` | UNIQUE INDEX | `(alias_scope, alias)` |
| `idx_music_title_alias_scope_alias` | `music_title_alias` | INDEX | `(alias_scope, alias)` |
| `uq_music_title_alias_textage_scope_alias` | `music_title_alias` | UNIQUE INDEX | `(textage_id, alias_scope, alias)` |

## `latest.json` 仕様

### キー定義

| キー | 型 | 必須 | 説明 |
| --- | --- | --- | --- |
| `file_name` | `string` | 必須 | 生成 SQLite ファイル名 |
| `schema_version` | `string` | 必須 | スキーマバージョン |
| `generated_at` | `string` | 必須 | UTC ISO8601 (`Z` suffix) |
| `sha256` | `string` | 必須 | SQLite 実体の SHA-256 |
| `byte_size` | `number` | 必須 | SQLite 実体サイズ（bytes） |
| `source_hashes` | `object` | 任意 | Textage 3 ソース + AC/INF manual alias CSV の SHA-256 |

### `source_hashes` サブキー

| キー | 型 | 説明 |
| --- | --- | --- |
| `titletbl.js` | `string` | `titletbl.js` の SHA-256 |
| `datatbl.js` | `string` | `datatbl.js` の SHA-256 |
| `actbl.js` | `string` | `actbl.js` の SHA-256 |
| `manual_alias_ac_csv` | `string` | `music_alias_manual_ac_csv_path` で指定した CSV の SHA-256 |
| `manual_alias_inf_csv` | `string` | `music_alias_manual_inf_csv_path` で指定した CSV の SHA-256 |

### `latest.json` 整合性検証

| 検証項目 | 条件 |
| --- | --- |
| `file_name` | 実在する SQLite ファイル名と一致 |
| `sha256` | SQLite 実体ハッシュと一致 |
| `byte_size` | SQLite 実体サイズと一致 |
| `schema_version` | DB 内 `meta.schema_version` と一致 |

### 運用ルール（重要）

| ルール | 内容 |
| --- | --- |
| 最新判定 | タグ名ではなく `latest.json.file_name` を正とする |
| 複数リリース/日 | タグは `YYYY-MM-DD`, `YYYY-MM-DD.N` で増える |
| SQLite ファイル名 | 日付のみ（`.N` suffix は付かない） |

## AC スコア取り込み生成物（`src/ac_score_import.py`）

### 生成物一覧

| 生成物 | 既定名 | 役割 |
| --- | --- | --- |
| レポート JSON | `import_report.json` | 同定結果のサマリ |
| 未一致 CSV | `unmatched_titles.csv` | 未一致タイトルの件数一覧 |

### `import_report.json` キー定義

| キー | 型 | 説明 |
| --- | --- | --- |
| `source_csv_file` | `string` | 入力 CSV パス |
| `alias_scope` | `string` | 固定で `ac` |
| `total_song_rows` | `number` | 総行数 |
| `matched_song_rows` | `number` | 同定成功行数 |
| `unmatched_song_rows` | `number` | 同定失敗行数 |
| `match_rate` | `number` | 一致率（%） |
| `unmatched_titles_topN` | `array` | 未一致上位（最大 10 件） |
| `generated_at` | `string` | UTC ISO8601 (`Z` suffix) |

### `unmatched_titles.csv` 列定義

| 列名 | 型 | 説明 |
| --- | --- | --- |
| `title` | `string` | 未一致タイトル |
| `count` | `number` | 出現回数 |

### Discord 通知フォールバック

| 段階 | 内容 |
| --- | --- |
| 1 | 未一致 Top10 を本文に含めて送信 |
| 2 | 長すぎる場合は Top5 に縮小 |
| 3 | さらに長い場合は未一致一覧を省略 |

## ビルドフロー（`main.py`）

| 手順 | 処理 | 生成/検証物 |
| --- | --- | --- |
| 1 | `settings.yaml` 読み込み | 設定 |
| 2 | 最新リリースから前回 SQLite / `latest.json` 取得（必要時） | 基準データ |
| 3 | Textage 3 ソース取得 + AC/INF manual alias CSV ハッシュ計算 | `source_hashes` |
| 4 | 5 ハッシュ完全一致ならスキップ | スキップ通知 |
| 5 | SQLite 更新生成 | `song_master_YYYY-MM-DD.sqlite` |
| 6 | DB 制約/データ整合性検証 | DB 検証 |
| 7 | `latest.json` 生成 + 実体突合 | `latest.json` |
| 8 | `upload_to_release=true` 時に日付タグリリースへアップロード | Releases 資産 |

## 設定（`settings.yaml`）

### ルート設定

| キー | 既定値 | 説明 |
| --- | --- | --- |
| `output_db_path` | `song_master.sqlite` | 出力先ディレクトリとファイル stem |
| `schema_version` | `33` | `meta` / `latest.json` に反映 |
| `chart_id_missing_policy` | `error` | 旧 DB 比較で欠損時の動作（`error` / `warn`） |
| `music_alias_manual_ac_csv_path` | `data/music_alias_manual_ac.csv` | AC 用手動エイリアス CSV パス |
| `music_alias_manual_inf_csv_path` | `data/music_alias_manual_inf.csv` | INFINITAS 用手動エイリアス CSV パス |

### `github` 設定

| キー | 既定値 | 説明 |
| --- | --- | --- |
| `owner` | `tts1374` | リポジトリ owner |
| `repo` | `iidx_all_songs_master` | リポジトリ名 |
| `upload_to_release` | `true` | `main.py` 内で公開まで実施するか |
| `require_previous_release` | `true` | 前回リリース取得を必須にするか |
| `asset_name` | `song_master.sqlite` | フォールバック用資産名 |

## 手動エイリアス CSV 仕様（`data/music_alias_manual_ac.csv` / `data/music_alias_manual_inf.csv`）

### 列定義

| 列名 | 必須 | 許容値/形式 | 備考 |
| --- | --- | --- | --- |
| `textage_id` | 必須 | `music.textage_id` に存在する値 |  |
| `alias` | 必須 | 非空文字列 |  |
| `alias_scope` | 必須 | `ac` / `inf` |  |
| `alias_type` | 必須 | `manual` 固定 |  |
| `note` | 任意 | 任意文字列 | ロジック未使用 |
| `title_canon` | 任意 | 任意文字列 | 読み込むがロジック未使用 |

### 検証ルール

| ルール | 内容 |
| --- | --- |
| 必須列/必須値 | 欠落・空値はエラー |
| `alias_scope` | `ac` / `inf` 以外はエラー |
| `alias_type` | `manual` 以外はエラー |
| CSV 内重複 | `(alias_scope, alias)` 重複はエラー |
| 孤立参照 | `music` に存在しない `textage_id` はエラー |
| DB 衝突 | 一意制約衝突はエラー |

運用上は `music_alias_manual_ac.csv` は `alias_scope=ac`、`music_alias_manual_inf.csv` は `alias_scope=inf` を推奨します。

## 実行方法

### 前提

| 項目 | 内容 |
| --- | --- |
| Python | 3.11+（CI は 3.11） |
| 依存導入 | `pip install -r requirements.txt` |
| 必須環境変数 | `GITHUB_TOKEN`（`main.py` 実行時） |

### コマンド

| 用途 | コマンド |
| --- | --- |
| SQLite / `latest.json` 生成 | `python main.py` |
| AC スコア同定レポート生成 | `python src/ac_score_import.py <AC_SCORE_CSV_PATH> --sqlite-path song_master.sqlite --report-path import_report.json --unmatched-csv-path unmatched_titles.csv` |

## CI / リリース運用

ワークフロー: `.github/workflows/build_song_master.yml`

### 実行マトリクス

| トリガー | 実行内容 | 公開動作 |
| --- | --- | --- |
| `pull_request` | `pytest -m light` | なし |
| `push` | フルビルド + `pytest -m full` | ドラフトリリース作成 |
| `schedule` | フルビルド + `pytest -m full` | 自動公開なし（失敗通知のみ） |
| `workflow_dispatch` | フルビルド + `pytest -m full` | `publish=true` の場合のみドラフトリリース作成 |

### リリース方針

| 項目 | 方針 |
| --- | --- |
| latest タグ上書き | 無効 |
| 新規公開 | 常に日付タグで新規作成 |
| タグ形式 | `YYYY-MM-DD` / `YYYY-MM-DD.N` |
| アセット更新 | 置換ではなく追加アップロード |

