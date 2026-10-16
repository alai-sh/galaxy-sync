from galaxy_sync.plugins import plugin_lines


def extraction(plugins, routing=None):
    return {"plugins": plugins, "routing": routing or {}}


def test_since_until_ignore_publication_order_and_prereleases():
    a = {"type": "module", "name": "a", "short_description": "old desc"}
    a2 = {"type": "module", "name": "a", "short_description": "new desc"}
    b = {"type": "module", "name": "b"}
    extractions = {
        "12.0.0": extraction([a]),
        "13.0.0-rc.1": extraction([b]),
        "13.0.0": extraction([b]),
        "12.6.6": extraction([a2]),  # maintenance release published after 13.0.0
    }
    out = {line["name"]: line for line in plugin_lines("ns.coll", extractions)}
    assert out["a"]["since"] == "12.0.0"
    assert out["a"]["until"] == "13.0.0"
    assert out["a"]["short_description"] == "new desc"
    assert out["b"]["since"] == "13.0.0"
    assert "until" not in out["b"]


def test_routing_only_entries_come_from_latest_version():
    extractions = {
        "1.0.0": extraction(
            [{"type": "module", "name": "x"}], {"module:gone": {"tombstone": {"removal_version": "0.5.0"}}}
        ),
        "2.0.0": extraction([{"type": "module", "name": "x"}], {"module:moved": {"redirect": "other.coll.moved"}}),
    }
    out = plugin_lines("ns.coll", extractions)
    routing_only = [line for line in out if "since" not in line]
    assert routing_only == [
        {
            "v": 1,
            "type": "module",
            "name": "moved",
            "collection": "ns.coll",
            "routing": {"redirect": "other.coll.moved"},
        }
    ]
