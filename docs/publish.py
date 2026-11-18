"""Publish a built documentation into the branch that GitHub Pages serves.

The branch holds one directory per published version: ``v2.0.0`` for a release, ``1.x`` for a line
of releases documented once. The root of the site is a copy of the newest version, so the links
without a version keep working and ``latest`` always means the last release. ``versions.json`` at the
root lists the versions for the switcher of the pages.

Usage::

    python docs/publish.py --html docs/_build/html --version v2.0.0 --repo git@github.com:openweathermap/deker.git
"""

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile

from pathlib import Path

RELEASE = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")
LINE = re.compile(r"^(\d+)\.x$")
LINE_RANK = 10**6  # a line of releases is newer than any release of that line
KEPT_AT_ROOT = frozenset({".git", ".nojekyll", "CNAME", "versions.json", "dev"})
COMMITTER = "github-actions[bot] <41898282+github-actions[bot]@users.noreply.github.com>"


def version_key(name: str) -> tuple[int, int, int] | None:
    """Rank a published version by its name; ``None`` for a name that is not a version.

    :param name: name of a directory at the root of the branch
    :returns: a sortable key, or ``None``
    """
    release = RELEASE.match(name)
    if release:
        return (int(release.group(1)), int(release.group(2)), int(release.group(3)))
    line = LINE.match(name)
    if line:
        return (int(line.group(1)), LINE_RANK, LINE_RANK)
    return None


def published_versions(site: Path) -> list[str]:
    """List the versions published in the site, newest first.

    :param site: root of the branch checkout
    :returns: directory names of the versions
    """
    ranked: list[tuple[tuple[int, int, int], str]] = []
    for path in site.iterdir():
        key = version_key(path.name)
        if path.is_dir() and key is not None:
            ranked.append((key, path.name))
    return [name for _, name in sorted(ranked, reverse=True)]


def git(*args: str, cwd: Path) -> str:
    """Run a git command and return its output.

    :param args: arguments of git
    :param cwd: repository to run in
    :returns: standard output, stripped
    :raises subprocess.CalledProcessError: when git fails
    """
    completed = subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True)
    return completed.stdout.strip()


def checkout_branch(repo: str, branch: str, work_dir: Path) -> None:
    """Clone the branch of the site into the working directory, or start it empty when there is none.

    :param repo: git url or path of the repository
    :param branch: branch that GitHub Pages serves
    :param work_dir: empty directory for the checkout
    """
    git("init", "--quiet", "--initial-branch", branch, cwd=work_dir)
    git("remote", "add", "origin", repo, cwd=work_dir)
    fetched = subprocess.run(
        ["git", "fetch", "--quiet", "--depth", "1", "origin", branch], cwd=work_dir, capture_output=True, text=True
    )
    if fetched.returncode == 0:
        git("checkout", "--quiet", "-b", branch, f"origin/{branch}", cwd=work_dir)
        print(f"Branch {branch}: {git('log', '-1', '--format=%h %s', cwd=work_dir)}")
    else:
        print(f"Branch {branch} does not exist yet, it is created")


def replace_directory(target: Path, source: Path) -> None:
    """Replace the directory with a copy of the source.

    :param target: directory to replace
    :param source: directory to copy
    """
    if target.exists():
        shutil.rmtree(target)
    shutil.copytree(source, target, ignore=shutil.ignore_patterns(".doctrees", ".buildinfo"))


def sync_root(site: Path, latest: str) -> None:
    """Make the root of the site a copy of the newest version, keeping the other versions and the site files.

    :param site: root of the branch checkout
    :param latest: name of the newest version
    """
    for path in site.iterdir():
        if path.name in KEPT_AT_ROOT or version_key(path.name) is not None:
            continue
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
    for path in (site / latest).iterdir():
        if path.is_dir():
            shutil.copytree(path, site / path.name)
        else:
            shutil.copy2(path, site / path.name)


def write_site_files(site: Path, latest: str, versions: list[str], cname: str) -> None:
    """Write the files of the site root: the version list, the domain and the Jekyll switch.

    :param site: root of the branch checkout
    :param latest: name of the newest version
    :param versions: names of the published versions, newest first
    :param cname: custom domain of the site, empty for none
    """
    listing = {"latest": latest, "versions": [{"name": name, "url": f"/{name}/"} for name in versions]}
    (site / "versions.json").write_text(json.dumps(listing, indent=2) + "\n")
    (site / ".nojekyll").touch()
    if cname:
        (site / "CNAME").write_text(cname + "\n")


def commit_and_push(site: Path, branch: str, message: str, committer: str, push: bool) -> None:
    """Commit the site and push the branch.

    :param site: root of the branch checkout
    :param branch: branch that GitHub Pages serves
    :param message: commit message
    :param committer: ``Name <email>`` of the commit author
    :param push: push the branch after the commit
    """
    name, email = committer.rstrip(">").split(" <", 1)
    git("add", "--all", cwd=site)
    if not git("status", "--porcelain", cwd=site):
        print("Nothing changed")
        return
    git("-c", f"user.name={name}", "-c", f"user.email={email}", "commit", "--quiet", "--message", message, cwd=site)
    print(f"Committed: {git('log', '-1', '--format=%h %s', cwd=site)}")
    if push:
        git("push", "--quiet", "origin", branch, cwd=site)
        print(f"Pushed {branch}")


def publish(
    html: Path, version: str, repo: str, branch: str, cname: str, committer: str, push: bool, work_dir: Path
) -> None:
    """Publish the built documentation as a version of the site.

    :param html: directory with the built html
    :param version: name to publish under: a release tag or a line of releases
    :param repo: git url or path of the repository
    :param branch: branch that GitHub Pages serves
    :param cname: custom domain of the site, empty for none
    :param committer: ``Name <email>`` of the commit author
    :param push: push the branch after the commit
    :param work_dir: empty directory for the checkout
    :raises ValueError: when the version name is not a release tag or a line of releases
    """
    if version_key(version) is None:
        raise ValueError(f"{version!r} is neither a release tag (v2.0.0) nor a line of releases (1.x)")
    checkout_branch(repo, branch, work_dir)
    replace_directory(work_dir / version, html)
    versions = published_versions(work_dir)
    latest = versions[0]
    sync_root(work_dir, latest)
    write_site_files(work_dir, latest, versions, cname)
    print(f"Versions: {', '.join(versions)}; latest: {latest}")
    commit_and_push(work_dir, branch, f"Publish the documentation of {version}", committer, push)


def main(argv: list[str] | None = None) -> int:
    """Parse the arguments and publish.

    :param argv: command line arguments, ``None`` for ``sys.argv``
    :returns: exit code
    """
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--html", type=Path, required=True, help="directory with the built html")
    parser.add_argument("--version", required=True, help="name to publish under: v2.0.0 or 1.x")
    parser.add_argument("--repo", required=True, help="git url or path of the repository")
    parser.add_argument("--branch", default="gh-pages", help="branch that GitHub Pages serves")
    parser.add_argument("--cname", default="docs.deker.io", help="custom domain of the site, empty for none")
    parser.add_argument("--committer", default=COMMITTER, help="Name <email> of the commit author")
    parser.add_argument("--no-push", action="store_true", help="commit, but do not push")
    parser.add_argument("--work-dir", type=Path, help="keep the checkout here instead of a temporary directory")
    args = parser.parse_args(argv)
    if args.work_dir:
        args.work_dir.mkdir(parents=True, exist_ok=True)
        work_dir = args.work_dir
    else:
        work_dir = Path(tempfile.mkdtemp(prefix="deker-docs-"))
    try:
        publish(args.html, args.version, args.repo, args.branch, args.cname, args.committer, not args.no_push, work_dir)
    finally:
        if not args.work_dir:
            shutil.rmtree(work_dir, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
