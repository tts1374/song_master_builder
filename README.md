# iidx_all_songs_master


IIDXの楽曲マスターデータ（SQLite）を生成・更新し、GitHub Releases に配布するためのシステムです。

本リポジトリの主目的は「プログラム」そのものではなく、生成物である `song_master.sqlite` を提供することです。  
大会運営・エビデンス管理・リザルト集計など、IIDX楽曲情報を参照するシステムの共通マスタとして利用できます。

---

## 生成物（配布物）

### song_master.sqlite

本システムが生成するSQLiteデータベースです。

- 曲情報（タイトル、アーティスト、ジャンル、収録状況など）
- 譜面情報（SP/DP、難易度、レベル、ノーツ数、譜面有効フラグ）

を含みます。

---

## データソース

Textage の以下ファイルを取得してSQLiteへ反映します。

- `titletbl.js` : 曲情報（タイトル/アーティスト/ジャンル/version/textage_id）
- `datatbl.js`  : ノーツ数
- `actbl.js`    : 譜面レベル / AC収録フラグ / INFINITAS収録フラグ

---

## DB仕様

### music テーブル（曲情報）

| column | type | description |
|--------|------|-------------|
| music_id | INTEGER | 内部ID |
| textage_id | TEXT | Textage恒久ID（ユニーク） |
| version | TEXT | 収録バージョン（例: `33`, `SS`） |
| title | TEXT | 曲名 |
| artist | TEXT | アーティスト |
| genre | TEXT | ジャンル |
| is_ac_active | INTEGER | AC収録フラグ（0/1） |
| is_inf_active | INTEGER | INFINITAS収録フラグ（0/1） |
| last_seen_at | TEXT | Textage取得時に確認された最終日時 |
| created_at | TEXT | 初回登録日時 |
| updated_at | TEXT | 更新日時 |

---

### chart テーブル（譜面情報）

| column | type | description |
|--------|------|-------------|
| chart_id | INTEGER | 内部ID |
| music_id | INTEGER | music参照 |
| play_style | TEXT | `SP` / `DP` |
| difficulty | TEXT | `BEGINNER` / `NORMAL` / `HYPER` / `ANOTHER` / `LEGGENDARIA` |
| level | INTEGER | 譜面レベル |
| notes | INTEGER | ノーツ数 |
| is_active | INTEGER | 譜面有効フラグ（0/1） |
| last_seen_at | TEXT | Textage取得時に確認された最終日時 |
| created_at | TEXT | 初回登録日時 |
| updated_at | TEXT | 更新日時 |

---

## データ更新仕様

本システムは毎回DBを作り直すのではなく、以下の動作で整合性を維持します。

### 1. GitHub Releases の最新SQLiteを取得（存在する場合）

- latest release の asset から `song_master.sqlite` をダウンロード
- 存在しなければローカル新規作成

### 2. 収録フラグを一旦リセット

更新開始時に `music.is_ac_active` / `music.is_inf_active` を一旦 `0` にします。  
その後Textageデータに存在する曲のみフラグを立て直します。

これにより、Textage側から削除された曲は「未収録扱い」として残ります。

### 3. Upsert（存在すれば更新、無ければ追加）

- `music` は `textage_id` をキーに Upsert
- `chart` は `(music_id, play_style, difficulty)` をキーに Upsert

---

## 正規化処理

Textage由来の文字列は、DB保存時に以下を正規化します。

- HTMLタグ除去  
  例: `<br>` や `<span ...>` を除去
- HTML文字実体参照のデコード  
  例: `&#332;` → `Ō`
- 空白の正規化

これにより、DB上での検索・一致判定を安定させます。

---

### GitHub Actions での運用

本システムは以下の用途での自動運用を想定しています。

定期実行（毎月1日 03:00 JST）

### Textage更新検知後の更新

SQLite生成 → Releaseへアップロード

### 注意事項

Textageデータ構造変更が発生した場合、取得処理が動かなくなる可能性があります。

本DBは公式データではありません。

is_ac_active / is_inf_active はTextageのフラグを元にしており、完全一致を保証するものではありません。

### ライセンス

本リポジトリのコードはリポジトリ内のLICENSEに従います。
Textageおよび楽曲情報は各権利者に帰属します。
