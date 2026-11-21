"""复检系统测试：净重口径规则、更正后的增量复算边界、并发与持久化。"""

import io
import os
import random
import sqlite3
import sys
import tempfile
import threading
import urllib.request

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("FILLING_DB",
                      os.path.join(tempfile.gettempdir(), "filling_import_default.db"))

from app import create_app, parse_net_csv, NOMINAL_GRAMS, TRIGGER_GRAMS  # noqa: E402


# ------------------------------------------------------------------ 工具

def reference(weights):
    """全量参考实现：weights {seq: net_weight} -> (events, states)。"""
    cum, rs = 0, None
    events, states = [], {}
    for seq in sorted(weights):
        cum += NOMINAL_GRAMS - weights[seq]
        if cum < 0:
            cum = 0
        if cum == 0:
            rs = None
        else:
            if rs is None:
                rs = seq
            if cum >= TRIGGER_GRAMS:
                events.append((rs, seq, cum))
                cum, rs = 0, None
        states[seq] = (cum, rs)
    return events, states


def db(app, sql, args=()):
    conn = sqlite3.connect(app.config["DB_PATH"])
    try:
        return conn.execute(sql, args).fetchall()
    finally:
        conn.close()


def snapshot(app):
    return (
        db(app, "SELECT seq, net_weight, source FROM weighings ORDER BY seq"),
        db(app, "SELECT seq, cumulative, round_start FROM run_state ORDER BY seq"),
        db(app, "SELECT id, batch_id, start_seq, end_seq, grams FROM events ORDER BY id"),
        db(app, "SELECT id, op_type, rows, additions, corrections, first_changed, "
                "resumed_at FROM batches ORDER BY id"),
    )


def assert_matches_reference(app, weights):
    exp_events, exp_states = reference(weights)
    got_w = dict(db(app, "SELECT seq, net_weight FROM weighings"))
    assert got_w == weights
    got_states = {s: (c, r) for s, c, r in db(
        app, "SELECT seq, cumulative, round_start FROM run_state")}
    assert got_states == exp_states
    got_events = [(s, e, g) for s, e, g in db(
        app, "SELECT start_seq, end_seq, grams FROM events ORDER BY start_seq, end_seq")]
    assert got_events == exp_events


def api_upload(client, rows):
    body = "\n".join(f"{s},{w}" for s, w in rows)
    return client.post("/api/upload", data=body, content_type="text/csv")


@pytest.fixture()
def app(tmp_path):
    return create_app(str(tmp_path / "test.db"))


@pytest.fixture()
def client(app):
    return app.test_client()


# ------------------------------------------------------------------ 基本规则

def test_basic_trigger_and_reset(client, app):
    r = api_upload(client, [(i, 494) for i in range(1, 10)])
    assert r.status_code == 200
    # 每袋差 5 克：1-4 累计到 20 触发，5-8 再次触发，9 累计 5
    assert_matches_reference(app, {i: 494 for i in range(1, 10)})
    events = db(app, "SELECT start_seq, end_seq, grams FROM events ORDER BY id")
    assert events == [(1, 4, 20), (5, 8, 20)]
    st = client.get("/api/state").get_json()
    assert st["cumulative"] == 5 and st["round_start"] == 9 and st["last_seq"] == 9


def test_negative_reset_and_round_start(client, app):
    api_upload(client, [(1, 490), (2, 520), (3, 494), (4, 503)])
    # 1: +9 -> 9(起点1)；2: -21 -> 归零；3: +5 -> 5(起点3)；4: -4 -> 1
    st = client.get("/api/state").get_json()
    assert st["cumulative"] == 1 and st["round_start"] == 3
    assert_matches_reference(app, {1: 490, 2: 520, 3: 494, 4: 503})


def test_exact_zero_clears_round(client, app):
    api_upload(client, [(1, 490), (2, 508), (3, 494)])
    rows = db(app, "SELECT seq, cumulative, round_start FROM run_state ORDER BY seq")
    assert rows == [(1, 9, 1), (2, 0, None), (3, 5, 3)]


def test_single_weighing_trigger(client, app):
    api_upload(client, [(1, 470)])  # 差 29 克，单次即触发
    events = db(app, "SELECT start_seq, end_seq, grams FROM events")
    assert events == [(1, 1, 29)]
    st = client.get("/api/state").get_json()
    assert st["cumulative"] == 0 and st["round_start"] is None


def test_header_and_form_upload(client, app):
    data = {"file": (io.BytesIO("序号,克重\n1,494\n2,494\n3,494\n4,494".encode()),
                     "w.csv")}
    r = client.post("/upload", data=data,
                    content_type="multipart/form-data", follow_redirects=True)
    assert r.status_code == 200
    assert_matches_reference(app, {i: 494 for i in range(1, 5)})


def test_index_page(client, app):
    api_upload(client, [(i, 494) for i in range(1, 6)])
    r = client.get("/")
    assert r.status_code == 200
    assert "复检清单" in r.get_data(as_text=True)
    assert "包装批皮重" in r.get_data(as_text=True)


# ------------------------------------------------------------------ 录入约束与原子性

def test_append_only_enforced_and_atomic(client, app):
    api_upload(client, [(10, 490), (11, 495), (12, 496)])
    before = snapshot(app)
    # 序号 7 不存在且 <= 当前最大序号 12：整批拒绝，合法的 13 也不生效
    r = api_upload(client, [(13, 490), (7, 490)])
    assert r.status_code == 400
    assert "新号只往后追加" in r.get_json()["error"]
    assert snapshot(app) == before


def test_correction_of_existing_seq_allowed(client, app):
    api_upload(client, [(1, 490), (2, 495)])
    r = api_upload(client, [(1, 480)])  # 同号更正允许
    assert r.status_code == 200
    assert r.get_json()["corrections"] == 1
    assert_matches_reference(app, {1: 480, 2: 495})


def test_invalid_csv_atomic(client, app):
    api_upload(client, [(1, 490), (2, 495)])
    before = snapshot(app)
    r = client.post("/api/upload", data="3,abc", content_type="text/csv")
    assert r.status_code == 400
    assert snapshot(app) == before


def test_duplicate_seq_in_file_rejected(client, app):
    r = client.post("/api/upload", data="1,490\n1,491", content_type="text/csv")
    assert r.status_code == 400
    assert db(app, "SELECT COUNT(*) FROM weighings") == [(0,)]


def test_parse_csv_units():
    assert parse_net_csv("序号,克重\n1,490\n") == [(1, 490)]
    with pytest.raises(ValueError):
        parse_net_csv("1,490\n2\n")
    with pytest.raises(ValueError):
        parse_net_csv("0,490")
    with pytest.raises(ValueError):
        parse_net_csv("")
    with pytest.raises(ValueError):
        parse_net_csv("1,490\n2,491,3\n")


# ------------------------------------------------------------------ 更正后的复算边界

THIRTY = [494, 494, 494, 494, 489, 520, 494, 494, 494, 494,
          505, 494, 489, 489, 520, 494, 494, 494, 494, 494,
          505, 505, 494, 494, 489, 494, 520, 494, 494, 494]


def test_correction_recompute_boundary(client, app):
    weights = {i: w for i, w in enumerate(THIRTY, start=1)}
    api_upload(client, sorted(weights.items()))
    before_states = {s: (c, r) for s, c, r in db(
        app, "SELECT seq, cumulative, round_start FROM run_state")}
    before_events = db(app, "SELECT id, start_seq, end_seq, grams FROM events ORDER BY id")

    weights[10] = 480  # 更正序号 10：494 -> 480
    r = api_upload(client, [(10, 480)])
    body = r.get_json()
    assert r.status_code == 200
    assert body["first_changed"] == 10

    # 首处改动前的称重状态原样保留（复算从 10 开始，不触碰 1-9）
    after_states = {s: (c, r) for s, c, r in db(
        app, "SELECT seq, cumulative, round_start FROM run_state")}
    for seq in range(1, 10):
        assert after_states[seq] == before_states[seq]
    # 触发序号在首处改动之前的旧事件整行保留（含自增 id）
    after_events = db(app, "SELECT id, start_seq, end_seq, grams FROM events ORDER BY id")
    assert ([e for e in after_events if e[2] < 10]
            == [e for e in before_events if e[2] < 10])
    # 整体结果与全量重算一致
    assert_matches_reference(app, weights)


def test_correction_converges_tail_reused(client, app):
    api_upload(client, [(i, 494) for i in range(1, 21)])
    before_events = db(app, "SELECT id, start_seq, end_seq, grams FROM events ORDER BY id")
    before_states = {s: (c, r) for s, c, r in db(
        app, "SELECT seq, cumulative, round_start FROM run_state")}

    # 序号 10 由 494 更正为 491：差值 +3，累计在 12 触发后归零，与旧结果汇合
    r = api_upload(client, [(10, 491)])
    body = r.get_json()
    assert body["first_changed"] == 10
    assert body["resumed_at"] == 13  # 自 13 起沿用旧结果，不再重读重量

    after_events = db(app, "SELECT id, start_seq, end_seq, grams FROM events ORDER BY id")
    # 汇合点之后的旧事件原样保留（id 不变即未重算）
    assert ([e for e in after_events if e[2] >= 13]
            == [e for e in before_events if e[2] >= 13])
    # 受影响区间内旧事件 (9,12,20) 被替换为 (9,12,23)
    assert (9, 12, 23) in [(s, e, g) for _, s, e, g in after_events]
    # 汇合点之后的旧状态原样保留
    after_states = {s: (c, r) for s, c, r in db(
        app, "SELECT seq, cumulative, round_start FROM run_state")}
    for seq in range(13, 21):
        assert after_states[seq] == before_states[seq]
    assert_matches_reference(app, {**{i: 494 for i in range(1, 21)}, 10: 491})


def test_mixed_correction_and_additions(client, app):
    """同一批内既有更正又有追加：汇合后旧尾段沿用，新增序号从旧末尾状态继续。"""
    api_upload(client, [(i, 494) for i in range(1, 21)])
    before_events = db(app, "SELECT id, start_seq, end_seq, grams FROM events ORDER BY id")

    r = api_upload(client, [(10, 491), (21, 494), (22, 494)])
    body = r.get_json()
    assert body["additions"] == 2 and body["corrections"] == 1
    assert body["resumed_at"] == 13

    after_events = db(app, "SELECT id, start_seq, end_seq, grams FROM events ORDER BY id")
    assert ([e for e in after_events if 13 <= e[2] <= 20]
            == [e for e in before_events if 13 <= e[2] <= 20])
    weights = {**{i: 494 for i in range(1, 23)}, 10: 491}
    assert_matches_reference(app, weights)
    st = client.get("/api/state").get_json()
    assert st["cumulative"] == 10 and st["round_start"] == 21


def test_noop_correction_no_recompute(client, app):
    api_upload(client, [(i, 494) for i in range(1, 11)])
    before = snapshot(app)
    r = api_upload(client, [(5, 494)])  # 同号同净重同来源：无实际改动
    body = r.get_json()
    assert r.status_code == 200
    assert body["corrections"] == 0 and body["first_changed"] is None
    after = snapshot(app)
    assert after[0] == before[0] and after[1] == before[1] and after[2] == before[2]


# ------------------------------------------------------------------ 持久化与并发

def test_persistence_across_restart(tmp_path):
    dbfile = str(tmp_path / "restart.db")
    c1 = create_app(dbfile).test_client()
    api_upload(c1, [(i, 494) for i in range(1, 9)])
    expected_state = c1.get("/api/state").get_json()
    expected_events = c1.get("/api/events").get_json()
    # 模拟重启：同一数据库文件新建应用实例
    c2 = create_app(dbfile).test_client()
    assert c2.get("/api/state").get_json() == expected_state
    assert c2.get("/api/events").get_json() == expected_events


def test_failed_upload_keeps_last_success(client, app):
    api_upload(client, [(i, 494) for i in range(10, 18)])
    before = snapshot(app)
    client.post("/api/upload", data="18,not_a_number", content_type="text/csv")
    client.post("/api/upload", data="3,490", content_type="text/csv")  # 新号 <= 最大序号，拒绝
    assert snapshot(app) == before  # 最近一次成功结果原样可查


def test_concurrent_uploads(app):
    """8 个线程同时上传互不相交的新号段：按生效顺序串行处理。

    大号段若先生效，小号段会按「新号只往后追加」被合法拒绝；
    最终库内容必须与所有成功批次的全量参考计算一致，且每次上传要么整体生效要么整体拒绝。
    """
    from werkzeug.serving import make_server

    server = make_server("127.0.0.1", 0, app, threaded=True)
    port = server.socket.getsockname()[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        def post(rows):
            body = "\n".join(f"{s},{w}" for s, w in rows).encode()
            req = urllib.request.Request(
                f"http://127.0.0.1:{port}/api/upload", data=body,
                headers={"Content-Type": "text/csv"})
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    return resp.status
            except urllib.error.HTTPError as exc:
                return exc.code

        assert post([(i, 495) for i in range(1, 6)]) == 200
        weights = {i: 495 for i in range(1, 6)}
        results, errors = {}, []

        def worker(i):
            rows = [(100 * i + k, 488 + (k % 9)) for k in range(1, 11)]
            try:
                results[i] = post(rows)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(1, 9)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors
        assert all(code in (200, 400) for code in results.values())
        assert any(code == 200 for code in results.values())
        for i, code in results.items():
            if code == 200:  # 只累加实际生效的批次
                weights.update({100 * i + k: 488 + (k % 9) for k in range(1, 11)})
        assert_matches_reference(app, weights)
    finally:
        server.shutdown()
        thread.join()


# ------------------------------------------------------------------ 随机对照

def test_randomized_against_reference(app):
    client = app.test_client()
    rng = random.Random(20261121)
    weights = {}
    rows = [(i, rng.randint(470, 530)) for i in range(1, 61)]
    assert api_upload(client, rows).status_code == 200
    weights.update(rows)
    assert_matches_reference(app, weights)
    for _ in range(12):
        batch = {}
        if rng.random() < 0.75:  # 随机更正若干已存在序号
            for s in rng.sample(sorted(weights), k=rng.randint(1, 6)):
                batch[s] = rng.randint(470, 530)
        mx = max(weights)  # 随机追加新号
        for k in range(rng.randint(0, 4)):
            batch[mx + 1 + k] = rng.randint(470, 530)
        if not batch:
            continue
        assert api_upload(client, sorted(batch.items())).status_code == 200
        weights.update(batch)
        assert_matches_reference(app, weights)
