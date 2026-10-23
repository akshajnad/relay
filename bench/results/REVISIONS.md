# Revisions recorded in these results

The results files record the git revision their image was built from
(`image.revision`, from the image's `org.opencontainers.image.revision`
label), and some image tags contain it. Those revisions were recorded
before the branch's commit messages were reworded. The rewording did not
change any file: each commit below has the same tree and author
as the revision recorded for it.

| recorded revision | commit in this repository | recorded in |
|---|---|---|
| `373990c` | `102436f` | `chaos-sweep-raft-102436f.json` (image `relay:sweep-m3`) |
| `c302e0f` | `a6b5444` | `chaos-sweep-raft.json`, `chaos-sweep-redis.json` (image `relay:sweep-c302e0f`), `verification-a6b5444.txt` |
| `547dd3a` | `bf23b85` | `compare-20261027T033746Z.json` |
| `32aa118` | `d4ed0af` | `20261027T063007Z.json` |

The tree of each commit, which `git rev-parse <commit>^{tree}` prints
and which matched the recorded revision's tree when the messages were
reworded:

| commit | tree |
|---|---|
| `102436f` | `21580921bf16db881820d84afe59496210435443` |
| `a6b5444` | `8be5a181493360a903439ae47340a8a5ebd7cebe` |
| `bf23b85` | `94278182753534f1b95938697d0d276b958991d5` |
| `d4ed0af` | `a80162da91b0f88e4acea01efad225b2cf82ab95` |
