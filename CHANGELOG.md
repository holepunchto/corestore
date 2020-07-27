## 5.7.0
- Simplifies internal reference counting and namespace handling. This removes the `NamespacedCorestore` class, but does not alter the interface.
- Uses `refpool` for reference handling.
- Removes `Nanoguard` and the undocumented `this.guard` property on Corestore.
- Removes the private `_name` option to `Corestore.get` in favor of a public `name` option.
