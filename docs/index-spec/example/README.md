# Example index

A small index laid out exactly as an index root would be served. It's
illustrative only: no artifacts are included, and the `dl` URLs don't
resolve.

```
config.json
collections.json
plugins.jsonl                                     fully mocked plugin index
index/
  alai_example/demo.jsonl                         fully mocked native collection
  community/general.jsonl                         real data, 3 of 243 versions
  community/library_inventory_filtering_v1.jsonl  real data, all 10 versions
```

## What's real and what's mocked

- **`community/*.jsonl`:** real metadata from Galaxy's v3 API (fetched
  2026-10-10): versions, dependencies, sha256, sizes, `requires_ansible` and
  publication times. `general.jsonl` contains only the three most recent of
  community.general's 243 versions.
- **`alai_example/demo.jsonl`:** entirely mocked, to show cases the Galaxy
  data doesn't cover:
  - a yanked version (`0.1.1`);
  - a pre-release (`0.2.0-rc.1`);
  - a dependency with a range requirement on another index's namespace
    (`community.general` `>=13.0.0,<14.0.0`).

  Its `sha256` values are sha256 hashes of placeholder strings, not of real
  artifacts.
- **`plugins.jsonl`:** mocked entries for `alai_example.demo`, showing a
  current module, a module that was removed and redirected, a tombstoned
  lookup, two filters from one file, a role and a playbook.
- **`collections.json`:** generated from the index files above. Each
  `updated` is the newest `published` time in that file, and each `sha256`
  that file's SHA-256. In a real index `updated` also moves on yanks and
  deprecations.

## Things worth noticing

- **Lines are in publication order, not version order.**
  `community/general.jsonl` lists `13.4.0`, then `12.6.6`, then `13.5.0`,
  because the 12.x maintenance release was published after 13.4.0. Clients
  sort by version themselves.
- **One file per collection holds every version's dependencies.** Resolving
  `alai_example.demo` needs exactly three index files, which can all be
  fetched in parallel. Galaxy's API needs a request per candidate version.
- **`deps` keeps Ansible's requirement syntax as published.**

## Walkthrough: installing `alai_example.demo`

1. `GET config.json` gives format `1`, index template `index/{namespace}/{name}.jsonl`, and the `dl`
   template.
2. `GET collections.json` shows that this index has `alai_example.demo`, so
   it, and all its dependencies, resolve from this index.
3. `GET index/alai_example/demo.jsonl` gives these candidates, newest first:
   - `0.2.0`: chosen.
   - `0.2.0-rc.1`: a pre-release, skipped.
   - `0.1.1`: yanked, skipped.
   - `0.1.0`

   `0.2.0` depends on `community.general` `>=13.0.0,<14.0.0`.
4. `GET index/community/general.jsonl` gives `13.5.0`, the highest match. It
   depends on `community.library_inventory_filtering_v1` `>=1.0.0`.
5. `GET index/community/library_inventory_filtering_v1.jsonl` gives `1.1.5`.
6. Download all three tarballs through the `dl` template, verifying each
   sha256. For `community.general` 13.5.0, the template gives:

   ```
   https://dl.alai.sh/sha256/54/54598287e0ea0504aee2d32c884208ddf26f3ac41e0b4e773ff514af210787c4.tar.gz
   ```

On a warm cache, steps 1-5 are conditional requests that mostly return
`304 Not Modified`.
