# Changelog

## v0.3.1 - 2022-10-26

### Fixed

- 修复同步命令(`sync`)错误。
- 修复下载连接过期的问题。

### Added

- 删除命令(`remove`)支持`--file-id`参数。

## v0.3.0 - 2022-09-24

### Added

- 支持保存分享连接至本地

  可以将他人分享了连接保存至本地，而不需要保存在网盘。这只作为一个记录。在需要是提供查看搜索功能。

  使用这个功能，需要使用者在本地配置文件(`~/.alipcs-py/config.toml`)中配置:

  ```toml
  [share]
  store = true
  ```

  提供以下命令:

  | 命令               | 描述                         |
  | ------------------ | ---------------------------- |
  | storesharedlinks   | 保存分享连接至本地           |
  | listsharedlinks    | 显示本地保存的分享连接       |
  | listsharedfiles    | 显示本地保存的分享文件       |
  | findsharedlinks    | 查找本地保存的分享连接       |
  | findsharedfiles    | 查找本地保存的分享文件       |
  | findshared         | 查找本地保存的分享连接和文件 |
  | deletestoredshared | 删除本地保存的分享连接或文件 |
  | cleanstore         | 清理本地保存的无效分享连接   |

## v0.2.5 - 2022-01-15

### Updated

- 更新依赖。

### Changed

- 阿里网盘不支持单文件并发上传。`upload --upload-type One` 失效。

## v0.2.4 - 2021-10-12

### Added

- 支持 alywp.net 的分享连接。

### Fixed

- 修复 `upload -t One`
- 修复 `ls`, `download`, `play` 用于分享连接。

### Changed

- 改 `play` 命令选项 `--play` 缩写为 `--pl`

## v0.2.3 - 2021-09-24

### Fixed

- 重复列出分享根目录。

### Changed

- `ls`, `download`, `play` 命令用于分享 url 或 id 时，必须指定路径或 file id。

## v0.2.2 - 2021-09-23

### Fixed

- 修复保存分享连接根目录出错。

## v0.2.1 - 2021-09-23

### Added

- 支持下载、播放他人分享的文件。
- `listsharedpaths` 命令合并到 `ls`。
- 支持保存他人分享连接中的特定文件。

## v0.1.0 - 2021-09-19

Runnable version
