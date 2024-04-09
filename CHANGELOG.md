# Changelog

## v0.8.0 - 2024-04-09

Add new apis and remove unneeded apis.

#### Inner datas

1. **PcsFile** class

   - `path`

     Default is the name of the file. It will be different from different apis returned. See `AliPCSApi.meta`, `AliPCSApi.meta_by_path`, `AliPCSApi.get_file`, `AliPCSApi.list`, `AliPCSApi.list_iter`, `AliPCSApi.path_traceback`, `AliPCSApi.path`.

   - `update_download_url`

     The method is removed. Use `AliPCSApi.update_download_url` instead.

2. **FromTo** type

   The original `FromTo` is a nametuple. We change it to a general type `FromTo = Tuple[F, T]`

3. **PcsDownloadUrl** class

   - `expires`

     Add the method to check whether the `download_url` expires.

#### Errors

1. **AliPCSBaseError** class

   The base Exception class used for the PCS errors.

2. **AliPCSError(AliPCSBaseError)** class

   The error returned from alipan server when the client’s request is incorrect or the token is expired.

   It throw at **AliPCS** class when an error occurs.

3. **DownloadError(AliPCSBaseError)** class

   An error occurs when downloading action fails.

4. **UploadError(AliPCSBaseError)** class

   An error occurs when uploading action fails.

5. **RapidUploadError(UploadError)** class

   An error occurred when rapid uploading action fails.

6. **make_alipcs_error** function

   Make an AliPCSError instance.

7. **handle_error** function

   uses the `_error_max_retries` attribute of the wrapped method’s class to retry.

#### Core APIs

1. **AliPCS** class

   ```python
   class AliPCS:
       SHARE_AUTHS: Dict[str, SharedAuth] = {}
       def __init__(
           self,
           refresh_token: str,
           access_token: str = "",
           token_type: str = "Bearer",
           expire_time: int = 0,
           user_id: str = "",
           user_name: str = "",
           nick_name: str = "",
           device_id: str = "",
           default_drive_id: str = "",
           role: str = "",
           status: str = "",
           error_max_retries: int = 2,
           max_keepalive_connections: int = 50,
           max_connections: int = 50,
           keepalive_expiry: float = 10 * 60,
           connection_max_retries: int = 2,
       ): ...
   ```

   The core alipan.com service apis. It directly handles the raw requests and responses of the service.

   **New/Changed APIs are following:**

   - `path_traceback` method (**New**)

     Traceback the path of the file by its file_id. Return the list of all parent directories' info from the file to the top level directory.

   - `meta_by_path` method (**New**)

     Get meta info of the file by its path.

     > Can not get the shared files' meta info.

   - `meta` method (**Changed**)

     Get meta info of the file by its file_id.

   - `exists` method (**Changed**)

     Check whether the file exists. Return True if the file exists and does not in the trash else False.

   - `exists_in_trash` method (**New**)

     Check whether the file exists in the trash. Return True if the file exists in the trash else False.

   - `walk` method (**New**)

     Walk through the directory tree by its file_id.

   - `download_link` method (**Changed**)

     Get download link of the file by its file_id.

     First try to get the download link from the meta info of the file. If the download link is not in the meta info, then request the getting download url api.

2. **AliPCSApi** class

   ```python
   class AliPCSApi:
       def __init__(
           self,
           refresh_token: str,
           access_token: str = "",
           token_type: str = "",
           expire_time: int = 0,
           user_id: str = "",
           user_name: str = "",
           nick_name: str = "",
           device_id: str = "",
           default_drive_id: str = "",
           role: str = "",
           status: str = "",
           error_max_retries: int = 2,
           max_keepalive_connections: int = 50,
           max_connections: int = 50,
           keepalive_expiry: float = 10 * 60,
           connection_max_retries: int = 2,
       ): ...
   ```

   The core alipan.com service api with wrapped **AliPCS** class. It parses the raw content of response of AliPCS request into the inner data structions.

   - **New/Changed APIs are following:**

     - `path_traceback` method (**New**)

       Traceback the path of the file. Return the list of all `PcsFile`s from the file to the top level directory.

       > _Important_:
       > The `path` property of the returned `PcsFile` has absolute path.

     - `meta_by_path` method (**New**)

       Get the meta of the the path. Can not get the shared files' meta info by their paths.

       > _Important_:
       > The `path` property of the returned `PcsFile` is the argument `remotepath`.

     - `meta` method (**Changed**)

       Get meta info of the file.

       > _Important_:
       > The `path` property of the returned `PcsFile` is only the name of the file.

     - `get_file` method (**New**)

       Get the file's info by the given `remotepath` or `file_id`

       If the `remotepath` is given, the `file_id` will be ignored.

       > _Important_:
       > If the `remotepath` is given, the `path` property of the returned `PcsFile` is the `remotepath`.
       > If the `file_id` is given, the `path` property of the returned `PcsFile` is only the name of the file.

     - `exists` method (**Changed**)

       Check whether the file exists. Return True if the file exists and does not in the trash else False.

     - `exists_in_trash` method (**Changed**)

       Check whether the file exists in the trash. Return True if the file exists in the trash else False.

     - `list` method (**Changed**)

       List files and directories in the given directory (which has the `file_id`). The return items size is limited by the `limit` parameter. If you want to list more, using the returned `next_marker` parameter for next `list` call.

       > _Important_:
       > These PcsFile instances' path property is only the name of the file.

     - `list_iter` method (**Changed**)

       Iterate all files and directories at the directory (which has the `file_id`).

       > These returned PcsFile instances' path property is the path from the first sub-directory of the `file_id` to the file name.
       > e.g.
       > If the directory (owned `file_id`) has path `level0/`, a sub-directory which of path is
       > `level0/level1/level2` then its corresponding PcsFile.path is `level1/level2`.

     - `path` method (**Changed**)

       Get the pcs file's info by the given absolute `remotepath`

       > _Important_:
       > The `path` property of the returned `PcsFile` is the argument `remotepath`.

     - `list_path` method (**Removed**)

     - `list_path_iter` method (**Removed**)

     - `walk` method (**New**)

       Recursively Walk through the directory tree which has `file_id`.

       > _Important_:
       > These PcsFile instances' path property is the path from the first sub-directory of the `file_id` to the file.
       > e.g.
       > If the directory (owned `file_id`) has path `level0/`, a sub-directory which of path is
       > `level0/level1/level2` then its corresponding PcsFile.path is `level1/level2`.

     - `makedir` method (**Changed**)

       Make a directory in the `dir_id` directory

       > _Important_:
       > The `path` property of the returned `PcsFile` is only the name of the directory.

     - **makedir_path** method (**Changed**)

       Make a directory by the absolute `remotedir` path

       Return the list of all `PcsFile`s from the directory to the top level directory.

       > _Important_:
       > The `path` property of the returned `PcsFile` has absolute path.

     - `rename` method (**Changed**)

       Rename the file with `file_id` to `name`

       > _Important_:
       > The `path` property of the returned `PcsFile` is only the name of the file.

     - `copy` method (**Changed**)

       Copy `file_ids[:-1]` to `file_ids[-1]`

       > _Important_:
       > The `path` property of the returned `PcsFile` is only the name of the file.

     - `update_download_url` method (**New**)

       Update the download url of the `pcs_file` if it is expired.

       Return a new `PcsFile` with the updated download url.

#### Download

1. **MeDownloader** class

   ```python
   class MeDownloader:
       def __init__(
           self,
           range_request_io: RangeRequestIO,
           localpath: PathType,
           continue_: bool = False,
           max_retries: int = 2,
           done_callback: Optional[Callable[..., Any]] = None,
           except_callback: Optional[Callable[[Exception], Any]] = None,
       ) -> None: ...
   ```

2. **download** module

   - `DownloadParams` class (**Removed**)

     We remove the `DownloadParams` instead of using arguments for function calling.

   - `download_file` function (**Changed**)

     `download_file` downloads one remote file to one local directory. Raise any error occurred. So giving the upper level caller to handle errors.

   - `download` function (**Changed**)

     `download` function downloads any number of remote files/directory to one local directory. It uses a `ThreadPoolExecutor` to download files concurrently and raise the exception if any error occurred.

3. **upload** module

   - `UploadType` class (**Removed**)

     Alipan.com only support to upload a file through uploading slice parts one by one.

     So, the class is not needed.

   - `upload_file` function (**Changed**)

     Upload a file from one local file ( `from_to[0]`) to remote ( `from_to[1]`).

     First try to rapid upload, if failed, then upload file's slices.

     Raise exception if any error occurs.

   - `upload` function (**Changed**)

     Upload files in `from_to_list` to Alipan Drive.

     Use a `ThreadPoolExecutor` to upload files concurrently.

     Raise exception if any error occurs.

## v0.7.0 - 2024-04-03

### Updated

- `MeDownloader` 剥离线程池，让上层函数来控制。
- 不在 `upload_file` 函数上进行重试，让上层函数来控制。
- `PcsFile.update_download_url` 将在下个版本删除。请该用 `AliPCSApi.update_download_url`。
- `RangeRequestIO` 增加 `read_iter` 方法。
- 使用 `python-dateutil` 库来解析时间。

### Fixed

- 修复播放分享文件时的暂停问题。

## v0.6.3 - 2024-02-04

### Updated

- 减少上传时对文件检查的 api 请求。
- 支持 Python 3.8 ~ 3.12

## v0.6.2 - 2023-12-04

### Fixed

- 修复上传链接超时

### Updated

- 根新依赖

## v0.6.1 - 2023-05-09

### Fixed

- 修复下载进度条太长的问题。

## v0.6.0 - 2023-04-24

### New Feature

- 支持 阿里云盘开放平台 api。

## v0.5.3 - 2023-02-28

### Updated

- 因为 `device_id` 不再在 `AliPCS.user_info` 中返回，需要把运行环境中的 `device_id` 加入到 `user_info`。

## v0.5.2 - 2023-02-25

### Fixed

- 修复 `DeviceSessionSignatureInvalid` 报错。支持自动更新 `signature`。

## v0.5.1 - 2023-02-22

### Fixed

- 修复 `ls` 命令输出省略结果。 (#11)

## v0.5.0 - 2023-02-15

### Updated

- 使用临时 API 接口，让下载可用。

## v0.4.1 - 2023-02-02

### Fixed

- 修复安装失败。

### Updated

- 更新依赖。

## v0.4.0 - 2023-01-09

### Breaking Changes

- 下面几个 api 都增加了参数 `part_number`。

  - `AliPCS.create_file`
  - `AliPCS.prepare_file`
  - `AliPCSApi.create_file`
  - `AliPCSApi.prepare_file`

  `part_number` 指明上传的该文件需要分几次上传。
  这个参数需要用户自己计算出来。一般用 `ceiling(上传文件大小 / 单个上传分片大小)`，其中一般 `单个上传分片大小` 建议为 `80MB`。

### Fixed

- 修复上传大于 100G 的文件失败的问题。 (#4)
- 修复播出路径出错的问题。

## v0.3.4 - 2022-12-16

### Fixed

- 修复删除用户错误。 (#3)

## v0.3.3 - 2022-12-10

### Updated

- 更新 `AliPCS.meta` api。

## v0.3.2 - 2022-12-04

### Added

- `listsharedlinks` 命令支持分页。

### Changed

- 在下载和上传时，让调用者去初始化进度条。

### Fixed

- 修复不完整上传错误。
- 修复上传时创建多个同名目录的问题。
- 修复同步失败的问题。
- 修复 `ls`, `download`, `play`, `search` 中 `--include-regex` 选项。

### Updated

- 更新依赖。

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
