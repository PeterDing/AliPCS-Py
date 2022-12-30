import collections
import concurrent.futures
import pickle
from os import remove
from pathlib import Path
from queue import Queue
from threading import Lock, Thread
from time import sleep
from typing import Dict, List, Tuple

import platformdirs
from rich import print
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
)

from alipcs_py.alipcs import AliPCSApi
from alipcs_py.alipcs.inner import PcsFile
from alipcs_py.commands.log import get_logger

logger = get_logger(__name__)


class FindDisplay:
    def __init__(self) -> None:
        self.progress = Progress(
            TextColumn("[progress.description]{task.description}"),
            SpinnerColumn(),
            BarColumn(),
            MofNCompleteColumn(),
            auto_refresh=True,
        )

        self._task_thread = self.progress.add_task("Thread Num", total=0)
        self._task_dirs = self.progress.add_task("Dir Progress", total=0)
        self._task_pending = self.progress.add_task("Pending Queue", total=0)
        self._task_results = self.progress.add_task("Returned Result Queue", total=0)

        self.pending_total = 0
        self.pending_completed = 0

        self.results_total = 0
        self.results_completed = 0

        self._pending_lock = Lock()
        self._results_lock = Lock()

    def set_thread_num(self, num: int) -> None:
        self.progress.update(self._task_thread, total=num)

    def set_dir_time(self, num: int) -> None:
        self.progress.update(self._task_dirs, total=num)

    def start(self) -> None:
        self.progress.start()

    def thread_start(self):
        self.progress.update(self._task_thread, advance=1)

    def thread_stop(self) -> None:
        self.progress.update(self._task_thread, advance=-1)

    def pending_add(self) -> None:
        with self._pending_lock:
            self.pending_total += 1
            self.progress.update(self._task_pending, total=self.pending_total)

    def pending_finish(self) -> None:
        with self._pending_lock:
            self.pending_completed += 1
            self.progress.update(self._task_pending, advance=1)

    def results_add(self) -> None:
        with self._results_lock:
            self.results_total += 1
            self.progress.update(self._task_results, total=self.results_total)

    def results_finish(self) -> None:
        with self._results_lock:
            self.results_completed += 1
            self.progress.update(self._task_results, advance=1)

    def dirs_finish(self) -> None:
        self.progress.update(self._task_dirs, advance=1)

    def stop(self) -> None:
        self.progress.stop()

    def load(self, data: Tuple[int, int, int, int]) -> None:
        self.pending_total, self.pending_completed = data[0], data[1]
        self.results_total, self.results_completed = data[2], data[3]

        self.progress.update(
            self._task_pending,
            completed=self.pending_completed,
            total=self.pending_total,
        )
        self.progress.update(
            self._task_results,
            completed=self.results_completed,
            total=self.results_total,
        )

    def save(self) -> Tuple[int, int, int, int]:
        return (
            self.pending_total,
            self.pending_completed,
            self.results_total,
            self.results_completed,
        )


class Walker:
    def __init__(
        self, api: AliPCSApi, show_progress: bool, display: FindDisplay
    ) -> None:
        self.api = api
        self.groups: Dict[str, List[Tuple[str, str]]] = collections.defaultdict(list)
        self.show_progress = show_progress
        self.pending: Queue[Tuple[str, PcsFile]] = Queue()
        self.results: Queue[Tuple[str, List[PcsFile]]] = Queue()
        self.display = display
        self.finished = False

    def execute(self, times: int, thread_num: int) -> None:

        self._times = times
        self._lock_time = Lock()
        self.display.start()

        walk_threads: List[Thread] = [
            Thread(target=self.walking_thread, daemon=True) for _ in range(thread_num)
        ]
        for x in walk_threads:
            x.start()

        result_thread = Thread(target=self.handle_result, daemon=True)
        result_thread.start()

        watch_thread = Thread(target=self.watch, daemon=True)
        watch_thread.start()

        while True:
            self._lock_time.acquire(blocking=True)
            if self._times <= 0:
                self._lock_time.release()

                print("[green bold]Finished. Exiting...")

                # wait for the result thread to be finished
                result_thread.join()
                print("[green bold]Result thread completed.")

                # wait for walking threads to finish
                for x in walk_threads:
                    x.join()
                print("[green bold]Walking threads completed.")

                watch_thread.join()
                print("[green bold]Watch thread completed.")

                self.display.stop()
                return
            self._lock_time.release()

            # quit if all walking threads are dead
            alive_workers = 0
            for x in walk_threads:
                if x.is_alive():
                    alive_workers += 1
            if alive_workers == 0:
                with self._lock_time:
                    self._times = 0

            sleep(0.1)

    def watch(self) -> None:
        while True:
            sleep(1)
            with self._lock_time:
                if self._times <= 0:
                    return
            if self.pending.all_tasks_done.acquire(blocking=False):
                if self.results.all_tasks_done.acquire(blocking=False):
                    if self.show_progress:
                        print(
                            f"pending tasks:{self.pending.unfinished_tasks}, result tasks: {self.results.unfinished_tasks}"
                        )
                    if (
                        self.pending.unfinished_tasks == 0
                        and self.results.unfinished_tasks == 0
                    ):
                        with self._lock_time:
                            self._times = 0
                        self.finished = True
                    self.results.all_tasks_done.release()
                self.pending.all_tasks_done.release()

    def handle_result(self) -> None:
        fails = 0
        while True:
            with self._lock_time:
                if self._times <= 0:
                    return
            try:
                path, subs = self.results.get(block=True, timeout=2)
            except Exception as e:
                fails += 1
                if fails > 15:
                    print(
                        f"[red bold]Result Thread failed too many times. Exiting...{e}"
                    )
                    return
                continue
            fails = 0
            for sub in subs:
                if sub.is_file:
                    rapid_upload_info = sub.rapid_upload_info
                    content_hash = (
                        rapid_upload_info and rapid_upload_info.content_hash or ""
                    )
                    if content_hash != "":
                        self.groups[content_hash].append(
                            (sub.file_id, f"{path}/{sub.name}")
                        )
                else:
                    self.pending.put((path + "/" + sub.name, sub))
                    self.display.pending_add()

            if self.show_progress:
                print(f"Finished searching [u][white]{_fix_path(path)}")

            self.results.task_done()
            self.display.results_finish()

    def walking_thread(self) -> None:
        self.display.thread_start()
        fails = 0
        while True:
            with self._lock_time:
                if self._times <= 0:
                    self.display.thread_stop()
                    return
                self._times -= 1
                self.display.dirs_finish()
            try:
                path, now = self.pending.get(block=True, timeout=2)
            except Exception as e:
                fails += 1
                if fails > 10:
                    print(f"[red bold]Walk Thread failed too many times. Exiting...{e}")
                    self.display.thread_stop()
                    return
                continue
            fails = 0
            try:
                self.walk(path, now)
                self.pending.task_done()
                self.display.pending_finish()
            except Exception:
                # should mark as done despite Exception
                # as put here will add task count by one
                self.pending.task_done()
                self.pending.put((path, now))

    def walk(self, path: str, now: PcsFile) -> None:
        subs = [x for x in self.api.list_iter(now.file_id)]
        self.results.put((path, subs))
        self.display.results_add()

    def save(self) -> None:
        """After save, the walker is no longer valid!"""
        path = platformdirs.user_cache_path("alipcs_py")
        path.mkdir(parents=True, exist_ok=True)
        path = path / "duplicate-save.dat"
        with path.open("wb+") as f:
            save_pending = []
            save_results = []
            while not self.pending.empty():
                save_pending.append(self.pending.get())
                self.pending.task_done()  # maintain task count for constant execution
            while not self.results.empty():
                save_results.append(self.results.get())
                self.results.task_done()
            pickle.dump(
                (self.groups, save_pending, save_results, self.display.save()), f
            )
            print("[green bold]Progress saved.")

    def load(self) -> bool:
        path = platformdirs.user_cache_path("alipcs_py") / "duplicate-save.dat"
        if not path.exists():
            return False
        with path.open("rb") as f:
            self.groups, load_pending, load_results, load_display = pickle.load(f)
            for x in load_pending:
                self.pending.put(x)
            for x in load_results:
                self.results.put(x)
            self.display.load(load_display)
            print("[green bold]Progress loaded!")
            return True


def _fix_path(path: str) -> str:
    """Searching from root results in double slash at the beginning.
    This function removes one redundant slash.
    """
    return path[1:] if path[:2] == "//" else path


def find_all_duplicates(
    api: AliPCSApi,
    number: int,
    safe_rate: int,
    show_progress: bool,
    thread_num: int,
    skip: bool,
    output: bool,
    output_path: str,
) -> None:
    """Find all the duplicate file groups."""
    if show_progress:
        print("[green bold]Showing progress is enabled.")

    display = FindDisplay()
    display.set_dir_time(number)
    display.set_thread_num(thread_num)

    walker = Walker(api, show_progress, display)
    if not walker.load():
        root = api.path("/")
        if not root:
            print("[red bold]Get root failed!")
            return
        walker.pending.put(("/", root))
        display.pending_add()

    if skip:
        print("[green bold]Search skipped.")
    else:
        while not walker.finished and number > 0:
            if number > safe_rate:
                walker.execute(safe_rate, thread_num)
                number -= safe_rate
                walker.save()
                if not walker.finished:
                    walker.load()  # load to enable walker to run again
            else:
                walker.execute(number, thread_num)
                number = 0
                walker.save()

    print(f"[green bold]Current groups number: [cyan]{len(walker.groups)}")

    # now that all duplicates are found

    if not output:
        return

    print("[green bold]Starting to generate output.")

    # format:
    # group_id file_id path
    text = ""
    id = 0
    for x in walker.groups.values():
        if len(x) < 2:
            continue
        text += "\n".join([f"{id} {file_id} {_fix_path(path)}" for file_id, path in x])
        id += 1

    print("[green bold]Output generated.")
    if output_path == "":
        print(text)  # stdout
    else:
        dest = Path(output_path)
        with dest.open("w+") as f:
            f.write(text)
    print("[gold3 bold]Done.")


def drop() -> None:
    path = platformdirs.user_cache_path("alipcs_py") / "duplicate-save.dat"
    if path.exists():
        remove(path)
        print("[green bold]Dropped.")
    else:
        print("[green bold]Doesn't exist.")


def delete_file(api: AliPCSApi, delete_chunk: List[str], dry_run: bool) -> List[bool]:
    if dry_run:
        return [True] * len(delete_chunk)
    return api.remove(*delete_chunk)


def clean_duplicate(
    api: AliPCSApi, chunk_size: int, verbose: bool, dry_run: bool, threads: int
) -> None:
    walker = Walker(api, False, FindDisplay())

    if not walker.load():
        print("[red bold]Load result failed. Execute finddup first.")
        return

    if dry_run:
        print("[green bold]Dry run is enabled.")

    to_delete = []
    mapping: Dict[str, str] = {}  # file_id -> path

    for group in walker.groups.values():
        # group = (file_id, path)

        if len(group) < 2:
            continue

        if verbose:
            print(f"To keep [u white]{_fix_path(group[0][1])}")

        for x in group[1:]:
            to_delete.append(x[0])
            mapping[x[0]] = _fix_path(x[1])

            if verbose:
                print(f"To delete [u white]{_fix_path(x[1])}")

    delete_chunks = [
        to_delete[i : i + chunk_size] for i in range(0, len(to_delete), chunk_size)
    ]

    print("[green bold]Starting to delete...")

    progress = Progress(
        TextColumn("[progress.description]{task.description}"),
        SpinnerColumn(),
        BarColumn(),
        MofNCompleteColumn(),
    )
    delete_task = progress.add_task("Delete", total=len(delete_chunks))
    progress.start()

    delete_count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        future_to_chunk = {
            executor.submit(delete_file, api, chunk, dry_run): chunk
            for chunk in delete_chunks
        }
        for future in concurrent.futures.as_completed(future_to_chunk):
            chunk = future_to_chunk[future]
            progress.advance(delete_task, 1)
            try:
                res: List[bool] = future.result()
                for i, success in enumerate(res):
                    if success:
                        if verbose:
                            print(f"Delete [u white]{mapping[chunk[i]]}[/] success.")
                        delete_count += 1
                    else:
                        print(
                            f"[red bold]Delete [u white]{mapping[chunk[i]]}[/] failed."
                        )
            except Exception:
                for i, _ in enumerate(res):
                    print(f"[red bold]Delete [u white]{mapping[chunk[i]]}[/] failed.")

    progress.stop()

    print(f"[gold3 bold]Done. Deleted {delete_count} files in total.")
