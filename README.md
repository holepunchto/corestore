# neocorestore
![Test on Node.js](https://github.com/andrewosh/neocorestore/workflows/Test%20on%20Node.js/badge.svg)

A __WIP__, experimental take on Corestore. Still in-progress, but check out the tests for API diffs with v5.

*Note: The backup/restore features should only be used with extreme care until Hypercore handles fork recovery.*

These features might eventually become Corestore v6:
* Replaces derived-key-storage with a Hyperbee database of names.
* The API is now very name-centric. Every `get` operation must either provide a key or a name.
* No more `default` -- keep everything named.
* Namespacing is far more lightweight.
* Reference counting is far simpler, and is decoupled from namespaces.
* Backup/restore write capabilities from a "manifest" file.
* Transparently migrates from Corestore 5.



