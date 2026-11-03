from galaxy_sync import semver


def test_sorts_by_precedence():
    # The ordering itself is alai-core's; this checks the key hands it over.
    assert sorted(["1.10.0", "1.0.0", "1.0.0-rc.1", "1.2.0"], key=semver.sort_key) == [
        "1.0.0-rc.1",
        "1.0.0",
        "1.2.0",
        "1.10.0",
    ]


def test_invalid_versions_sort_lowest():
    assert sorted(["1.0.0", "banana", "apple"], key=semver.sort_key) == ["apple", "banana", "1.0.0"]
