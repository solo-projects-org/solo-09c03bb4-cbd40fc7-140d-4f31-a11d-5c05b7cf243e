"""毛重三列 CSV、包装批皮重登记/更正、来源切换与旧库迁移测试。"""

import io
import os
import sqlite3
import sys
import tempfile
import threading

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("FILLING_DB",
                      os.path.join(tempfile.gettempdir(), "filling_import_tare.db"))

from app import create_app, parse_gross_csv, parse_tare_csv  # noqa: E402


def db(app, sql, args=()):
    conn = sqlite3.connect(app.config["DB_PATH"])
    try:
        return conn.execute(sql, args).fetchall()
    finally:
        conn.close()


def post_csv(client, url, body):
    return client.post(url, data=body, content_type="text/csv")


@pytest.fixture()
def app(tmp_path):
    return create_app(str(tmp_path / "tare.db"))


@pytest.fixture()
def client(app):
    return app.test_client()


# ------------------------------------------------------------------ 解析

def test_parse_gross_and_tare():
    assert parse_gross_csv("序号,毛重,包装批号\n1,500,B01\n") == [(1, 500, "B01")]
    assert parse_tare_csv("包装批号,皮重\nB01,6\nB02,8\n") == [("B01", 6), ("B02", 8)]
    with pytest.raises(ValueError):
        parse_gross_csv("1,500\n")           # 列数不对
    with pytest.raises(ValueError):
        parse_gross_csv("1,500,\n")          # 缺包装批号
    with pytest.raises(ValueError):
        parse_gross_csv("1,500,B1\n1,501,B2\n")  # 文件内序号重复
    with pytest.raises(ValueError):
        parse_tare_csv("B01,-1\n")           # 负皮重
    with pytest.raises(ValueError):
        parse_tare_csv("")


# ------------------------------------------------------------------ 毛重扣皮

def test_gross_csv_deducts_tare_and_marks_source(client, app):
    r = post_csv(client, "/api/tare", "包装批号,皮重\nB01,6\nB02,8\n")
    assert r.status_code == 200 and r.get_json()["additions"] == 2
    r = post_csv(client, "/api/upload",
                 "抽检序号,毛重,包装批号\n1,500,B01\n2,490,B02\n3,505,B01\n")
    assert r.status_code == 200
    assert r.get_json()["op_type"] == "gross"
    rows = db(app,
              "SELECT seq, net_weight, gross_weight, batch_no, source "
              "FROM weighings ORDER BY seq")
    assert rows == [(1, 494, 500, "B01", "gross"),
                    (2, 482, 490, "B02", "gross"),
                    (3, 499, 505, "B01", "gross")]
    # 净重口径判定复检：1: +5, 2: +17 -> 22 在序号 2 触发；3: 0
    events = db(app, "SELECT start_seq, end_seq, grams FROM events")
    assert events == [(1, 2, 22)]
    st = client.get("/api/state").get_json()
    assert st["cumulative"] == 0 and st["round_start"] is None


def test_missing_tare_rejects_whole_operation(client, app):
    post_csv(client, "/api/tare", "B01,6\n")
    before = (db(app, "SELECT * FROM weighings"),
              db(app, "SELECT * FROM run_state"),
              db(app, "SELECT * FROM events"),
              db(app, "SELECT * FROM packaging_batches"),
              db(app, "SELECT id FROM batches"))
    r = post_csv(client, "/api/upload", "1,500,B01\n2,500,UNKNOWN\n")
    assert r.status_code == 400
    assert "尚未登记皮重" in r.get_json()["error"]
    after = (db(app, "SELECT * FROM weighings"),
             db(app, "SELECT * FROM run_state"),
             db(app, "SELECT * FROM events"),
             db(app, "SELECT * FROM packaging_batches"),
             db(app, "SELECT id FROM batches"))
    assert after == before  # 整次操作退回，连批次行都不留


def test_negative_net_rejects_gross_upload(client, app):
    post_csv(client, "/api/tare", "B01,10\n")
    r = post_csv(client, "/api/upload", "1,9,B01\n")  # 9-10 < 0
    assert r.status_code == 400
    assert "净重为负" in r.get_json()["error"]
    assert db(app, "SELECT COUNT(*) FROM weighings") == [(0,)]


def test_two_col_treated_as_direct_net(client, app):
    r = post_csv(client, "/api/upload", "1,494\n2,494\n")
    assert r.status_code == 200
    assert r.get_json()["op_type"] == "direct"
    rows = db(app, "SELECT seq, net_weight, gross_weight, batch_no, source "
                   "FROM weighings ORDER BY seq")
    assert rows == [(1, 494, None, None, "direct"),
                    (2, 494, None, None, "direct")]


# ------------------------------------------------------------------ 来源切换

def test_same_net_switches_source_on_reupload(client, app):
    """净重相同也要换来源：direct -> gross、gross -> direct 都算更正。"""
    post_csv(client, "/api/tare", "B01,6\n")
    post_csv(client, "/api/upload", "1,494\n2,494\n")  # 直接净重
    # 毛重 500 - 皮重 6 = 净重 494，净重与序号 1 原值相同
    r = post_csv(client, "/api/upload", "1,500,B01\n")
    body = r.get_json()
    assert r.status_code == 200
    assert body["corrections"] == 1 and body["additions"] == 0
    row = db(app, "SELECT net_weight, gross_weight, batch_no, source "
                  "FROM weighings WHERE seq = 1")[0]
    assert row == (494, 500, "B01", "gross")  # 来源已换成毛重扣皮

    # 再用两列同净重切回直接净重
    r = post_csv(client, "/api/upload", "1,494\n")
    assert r.get_json()["corrections"] == 1
    row = db(app, "SELECT net_weight, gross_weight, batch_no, source "
                  "FROM weighings WHERE seq = 1")[0]
    assert row == (494, None, None, "direct")

    # 完全一致的重复三列上传：无改动
    r = post_csv(client, "/api/upload", "1,494\n")
    assert r.get_json()["corrections"] == 0
    assert r.get_json()["first_changed"] is None


def test_gross_correction_keeps_append_rule(client, app):
    post_csv(client, "/api/tare", "B01,6\n")
    post_csv(client, "/api/upload", "10,500,B01\n11,500,B01\n")
    # 新号 9 <= 当前最大 11：整批拒绝
    r = post_csv(client, "/api/upload", "9,500,B01\n")
    assert r.status_code == 400 and "新号只往后追加" in r.get_json()["error"]
    # 同号毛重更正允许
    r = post_csv(client, "/api/upload", "10,496,B01\n")  # 净重 490
    assert r.status_code == 200 and r.get_json()["corrections"] == 1
    assert db(app, "SELECT net_weight FROM weighings WHERE seq = 10") == [(490,)]


# ------------------------------------------------------------------ 皮重更正

def test_tare_correction_reconverts_only_referencing_weighings(client, app):
    post_csv(client, "/api/tare", "B01,6\nB02,10\n")
    post_csv(client, "/api/upload",
             "1,500,B01\n2,500,B02\n3,510,B01\n")
    post_csv(client, "/api/upload", "4,494\n")  # 序号4为直接净重，两列单独上传
    # B01 皮重 6 -> 10：只换算序号 1、3；B02 与直接净重不受影响
    r = post_csv(client, "/api/tare", "B01,10\n")
    body = r.get_json()
    assert r.status_code == 200
    assert body["corrections"] == 1
    assert body["affected_seqs"] == [1, 3]
    rows = dict(db(app, "SELECT seq, net_weight FROM weighings"))
    assert rows == {1: 490, 2: 490, 3: 500, 4: 494}
    assert db(app, "SELECT net_weight FROM weighings WHERE seq = 4") == [(494,)]
    assert db(app, "SELECT tare FROM packaging_batches WHERE batch_no = 'B01'") == [(10,)]


def test_tare_correction_negative_rolls_back_everything(client, app):
    post_csv(client, "/api/tare", "B01,6\nB02,10\n")
    post_csv(client, "/api/upload", "1,500,B01\n2,505,B01\n3,500,B02\n")
    before = (db(app, "SELECT tare, batch_no FROM packaging_batches"),
              db(app, "SELECT seq, net_weight FROM weighings"),
              db(app, "SELECT * FROM run_state"),
              db(app, "SELECT * FROM events"))
    # 同次改两个包装批：B02 皮重 600 会让序号 3 净重为负 -> 整次退回
    r = post_csv(client, "/api/tare", "B01,8\nB02,600\n")
    assert r.status_code == 400 and "净重为负" in r.get_json()["error"]
    after = (db(app, "SELECT tare, batch_no FROM packaging_batches"),
             db(app, "SELECT seq, net_weight FROM weighings"),
             db(app, "SELECT * FROM run_state"),
             db(app, "SELECT * FROM events"))
    assert after == before
    # 皮重批次行也不得留下
    assert db(app, "SELECT COUNT(*) FROM batches WHERE op_type = 'tare'") == [(1,)]


def test_tare_noop_and_new_batch_without_refs(client, app):
    post_csv(client, "/api/tare", "B01,6\n")
    # 重复登记同皮重：无更正、无换算
    r = post_csv(client, "/api/tare", "B01,6\n")
    body = r.get_json()
    assert r.status_code == 200
    assert body["corrections"] == 0 and body["first_changed"] is None
    # 新包装批尚无称重引用：登记成功但不触发复算
    r = post_csv(client, "/api/tare", "B99,3\n")
    body = r.get_json()
    assert body["additions"] == 1 and body["affected_seqs"] == []
    assert db(app, "SELECT tare FROM packaging_batches WHERE batch_no = 'B99'") == [(3,)]


def test_tare_recompute_matches_full_reference(client, app):
    post_csv(client, "/api/tare", "B01,6\nB02,8\n")
    post_csv(client, "/api/upload",
             "\n".join(f"{i},{500 + (i % 3)},B0{1 + i % 2}" for i in range(1, 21)))
    # 更正 B01 皮重后，全库净重与逐袋参考累计必须一致
    r = post_csv(client, "/api/tare", "B01,12\n")
    assert r.status_code == 200
    weights = dict(db(app, "SELECT seq, net_weight FROM weighings"))
    cum, rs = 0, None
    exp_events, exp_states = [], {}
    for seq in sorted(weights):
        cum += 499 - weights[seq]
        if cum < 0:
            cum = 0
        if cum == 0:
            rs = None
        else:
            if rs is None:
                rs = seq
            if cum >= 20:
                exp_events.append((rs, seq, cum))
                cum, rs = 0, None
        exp_states[seq] = (cum, rs)
    got_states = {s: (c, r) for s, c, r in db(
        app, "SELECT seq, cumulative, round_start FROM run_state")}
    assert got_states == exp_states
    got_events = [(s, e, g) for s, e, g in db(
        app, "SELECT start_seq, end_seq, grams FROM events")]
    assert got_events == exp_events


# ------------------------------------------------------------------ 查询与网页

def test_lookup_api_and_page(client, app):
    post_csv(client, "/api/tare", "B01,6\n")
    post_csv(client, "/api/upload", "1,494\n")
    post_csv(client, "/api/upload", "2,500,B01\n")  # 净重 494
    r = client.get("/api/weighing/2")
    assert r.status_code == 200
    assert r.get_json() == {
        "ok": True, "seq": 2, "net_weight": 494, "gross_weight": 500,
        "batch_no": "B01", "source": "gross", "tare": 6}
    assert client.get("/api/weighing/99").status_code == 404
    page = client.get("/weighing?seq=2").get_data(as_text=True)
    assert "毛重扣皮" in page and "B01" in page and "500" in page
    page = client.get("/weighing?seq=1").get_data(as_text=True)
    assert "直接净重" in page
    assert client.get("/weighing?seq=abc").status_code == 200  # 错误提示而非崩溃


def test_conversion_listing_contains_gross_batch_tare(client, app):
    post_csv(client, "/api/tare", "B01,6\n")
    post_csv(client, "/api/upload", "1,500,B01\n2,510,B01\n")
    page = client.get("/").get_data(as_text=True)
    assert "毛重换算记录" in page
    assert "500" in page and "510" in page and "B01" in page
    r = client.get("/api/packaging-batches")
    assert r.status_code == 200 and r.get_json()[0]["batch_no"] == "B01"


def test_form_tare_upload(client, app):
    data = {"tare_file": (io.BytesIO("包装批号,皮重\nB01,6\n".encode()), "t.csv")}
    r = client.post("/tare", data=data,
                    content_type="multipart/form-data", follow_redirects=True)
    assert r.status_code == 200
    assert db(app, "SELECT tare FROM packaging_batches") == [(6,)]


# ------------------------------------------------------------------ 并发

def test_concurrent_tare_and_gross(app):
    """皮重登记与毛重上传并发：按生效顺序串行，失败保留原状态，无 500。"""
    from werkzeug.serving import make_server

    server = make_server("127.0.0.1", 0, app, threaded=True)
    port = server.socket.getsockname()[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        import urllib.request
        import urllib.error

        def call(url, body):
            req = urllib.request.Request(
                f"http://127.0.0.1:{port}{url}", data=body.encode(),
                headers={"Content-Type": "text/csv"})
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    return resp.status
            except urllib.error.HTTPError as exc:
                return exc.code

        codes = []

        def worker(url, body):
            codes.append(call(url, body))

        threads = [
            threading.Thread(target=worker, args=("/api/tare", "B01,6\n")),
            threading.Thread(target=worker, args=("/api/tare", "B01,10\n")),
            threading.Thread(target=worker, args=("/api/upload", "1,500,B01\n")),
            threading.Thread(target=worker, args=("/api/upload", "2,494\n")),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert codes and all(c in (200, 400) for c in codes)
        # 最终状态自洽：每条毛重净重 = 毛重 − 当前皮重，且均非负
        conn = sqlite3.connect(app.config["DB_PATH"])
        try:
            rows = conn.execute(
                "SELECT gross_weight, net_weight, batch_no, source "
                "FROM weighings WHERE source = 'gross'").fetchall()
            tares = dict(conn.execute(
                "SELECT batch_no, tare FROM packaging_batches").fetchall())
        finally:
            conn.close()
        for gross, net, batch_no, _src in rows:
            assert net == gross - tares[batch_no] >= 0
    finally:
        server.shutdown()
        thread.join()


# ------------------------------------------------------------------ 旧库迁移

def test_legacy_db_migrates_to_direct_net(tmp_path):
    dbfile = str(tmp_path / "legacy.db")
    # 按旧版结构手工建库并写入数据
    conn = sqlite3.connect(dbfile)
    conn.executescript("""
        CREATE TABLE batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            applied_at TEXT NOT NULL DEFAULT (datetime('now')),
            rows INTEGER NOT NULL, additions INTEGER NOT NULL,
            corrections INTEGER NOT NULL,
            first_changed INTEGER, resumed_at INTEGER);
        CREATE TABLE weighings (
            seq INTEGER PRIMARY KEY,
            weight INTEGER NOT NULL CHECK (weight >= 0));
        CREATE TABLE run_state (
            seq INTEGER PRIMARY KEY, cumulative INTEGER NOT NULL, round_start INTEGER);
        CREATE TABLE events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER NOT NULL REFERENCES batches(id),
            start_seq INTEGER NOT NULL, end_seq INTEGER NOT NULL, grams INTEGER NOT NULL);
        INSERT INTO batches (rows, additions, corrections) VALUES (2, 2, 0);
        INSERT INTO weighings (seq, weight) VALUES (1, 494), (2, 494);
        INSERT INTO run_state (seq, cumulative, round_start) VALUES (1, 5, 1), (2, 10, 1);
    """)
    conn.commit()
    conn.close()

    migrated = create_app(dbfile)
    client = migrated.test_client()
    rows = db(migrated, "SELECT seq, net_weight, gross_weight, batch_no, source "
                        "FROM weighings ORDER BY seq")
    assert rows == [(1, 494, None, None, "direct"),
                    (2, 494, None, None, "direct")]
    # 旧的 run_state / 批次记录仍可查
    assert db(migrated, "SELECT seq, cumulative FROM run_state ORDER BY seq") == [
        (1, 5), (2, 10)]
    assert db(migrated, "SELECT op_type FROM batches") == [("net",)]
    # 迁移后可继续在同一库上做毛重/皮重操作
    r = client.post("/api/tare", data="B01,6\n", content_type="text/csv")
    assert r.status_code == 200
    r = client.post("/api/upload", data="3,500,B01\n", content_type="text/csv")
    assert r.status_code == 200
    assert db(migrated, "SELECT net_weight, source FROM weighings WHERE seq = 3") == [
        (494, "gross")]
    # 再重启一次：已是新结构，迁移幂等
    again = create_app(dbfile)
    assert db(again, "SELECT COUNT(*) FROM weighings") == [(3,)]
