from galaxy_sync import semver


def test_precedence_follows_semver_spec():
    # Example ordering from the SemVer 2.0.0 spec.
    ordered = [
        "1.0.0-alpha",
        "1.0.0-alpha.1",
        "1.0.0-alpha.beta",
        "1.0.0-beta",
        "1.0.0-beta.2",
        "1.0.0-beta.11",
        "1.0.0-rc.1",
        "1.0.0",
        "1.10.0",
        "2.0.0",
    ]
    # Sort from the reverse order so the test can't pass by accident.
    reverse = ordered[::-1]
    assert sorted(reverse, key=semver.sort_key) == ordered


def test_build_metadata_tie_breaks_on_string():
    assert sorted(["1.0.0+b", "1.0.0+a", "1.0.0"], key=semver.sort_key) == ["1.0.0", "1.0.0+a", "1.0.0+b"]


def test_invalid_versions_sort_lowest():
    assert sorted(["1.0.0", "banana"], key=semver.sort_key) == ["banana", "1.0.0"]


def test_is_prerelease():
    assert semver.is_prerelease("0.2.0-rc.1")
    assert not semver.is_prerelease("33.0.484+git940b822")
