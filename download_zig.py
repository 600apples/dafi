#!/usr/bin/env python3
#
# Note: shebang defaults to python3 but this scripts also supports python2
#
import os
import sys
import shutil
import glob
import subprocess
import json
import re

script_dir = os.path.dirname(os.path.abspath(__file__))

def run_output(*args, **kwargs):
    print("[RUN] " + subprocess.list2cmdline(*args))
    sys.stdout.flush()
    try:
        return subprocess.check_output(*args, **kwargs).decode('ascii')
    except subprocess.CalledProcessError:
        return ""
def run(*args, **kwargs):
    print("[RUN] " + subprocess.list2cmdline(*args))
    sys.stdout.flush()
    return subprocess.check_call(*args, **kwargs)

def logrmtree(path):
    print("rm -rf '{}'".format(path))
    shutil.rmtree(path)

def mkdir(dir):
    if not os.path.exists(dir):
        os.mkdir(dir)

def prompt_int(msg, min, max):
    while True:
        sys.stdout.write("{} [{}-{}] ? ".format(msg, min, max))
        sys.stdout.flush()
        try:
            answer = int(sys.stdin.readline().strip())
        except ValueError:
            print("not an integer")
            continue
        if answer < min or answer > max:
            print("out of bounds")
            continue
        return answer

def find_prog(name):
    if os.name == "nt":
        results = run_output(["where", name]).splitlines()
        return None if len(results) == 0 else results[0]
    else:
        result = run_output(["which", name]).strip()
        return None if len(result) == 0 else result

def curl_download(url, file):
    run(["curl", url, "--output", file])

def urllib_request_download(url, file):
    import urllib.request
    print("urllib.request downloading '{}'...".format(url))
    with urllib.request.urlopen(url) as request:
        with open(file, "wb") as filed:
            filed.write(request.read())

def urllib2_download(url, file):
    import urllib2
    print("urllib2 downloading '{}'...".format(url))
    request = urllib2.urlopen(url)
    with open(file, "wb") as filed:
        filed.write(request.read())

def check_curl():
    if find_prog("curl"):
        return curl_download

def check_urllib_request():
    try:
        import urllib.request
        return urllib_request_download
    except ImportError:
        print("urllib.request module not found")
        return None

def check_urllib2():
    try:
        import urllib2
        return urllib2_download
    except ImportError:
        print("urllib2 module not found")
        return None

# TODO: add wget
downloaders = (
    # Try curl first, it seems to handle proxies and other things better than then the python libraries
    ("curl", check_curl),
    ("python3_urllib_request", check_urllib_request),
    ("python2_urllib2", check_urllib2),
)

def determine_download_func():
    #download_conf_filename = os.path.join(script_dir, "download.conf")
    #if os.path.exists(download_conf_filename):
    #    with open(download_conf_filename, "r") as file:
    #        download_conf_json = json.load(file)
    #    download_method = download_conf_json["download_method"]

    for name, check in downloaders:
        func = check()
        if func:
            return func

    names = ""
    prefix = ""
    for name,check in downloaders:
        names += prefix + name
        prefix = ", "
    sys.exit("Error: could not find an HTTP Client from this list: {}".format(names))

cached_download_func = None
def get_download_func():
    global cached_download_func
    if not cached_download_func:
        cached_download_func = determine_download_func()
        if not cached_download_func:
            sys.exit("Error: failed to find a way to download files via HTTP")
    return cached_download_func

def download_no_check(url, file):
    file_tmp = file + ".downloading"
    if os.path.exists(file_tmp):
        os.remove(file_tmp)
    get_download_func()(url, file_tmp)
    if os.path.exists(file):
        os.remove(file)
    os.rename(file_tmp, file)

def download_if_needed(url, file):
    if os.path.exists(file):
        print("file '{}' already downloaded".format(file))
    else:
        download_no_check(url, file)

def fetch_download_json():
    url = "https://ziglang.org/download/index.json"
    filename = "zigdownload.json"
    download_no_check(url, filename)
    with open(filename, "r") as file:
        return json.load(file)
    #os.remove(filename)

def get_latest_zig_url(platform):
    download_json = fetch_download_json()
    return download_json["master"][platform]["tarball"]

def find_zig_to_update():
    if os.name == "nt":
        basename = "zig.bat"
        candidates = run_output(["where", "zig.bat"]).splitlines()
        # TODO: should we check the bash contents?
    else:
        basename = "zig"
        zigs_in_path = run_output(["which", "-a", "zig"]).splitlines()
        candidates = []
        for zig in zigs_in_path:
            if os.path.islink(zig):
                link_target = os.readlink(zig)
                if re.match("zig[^/]+/zig", link_target):
                    candidates.append(zig)

    if len(candidates) == 1:
        print("using '{}'".format(candidates[0]))
        return candidates[0]

    if len(candidates) > 1:
        print("found multiple zig candidates in PATH")
        print("--------------------------------------------------------------------------------")
        file_index = 0
        for file in candidates:
            print("[{}] {}".format(file_index, file))
            file_index += 1
        selected_index = prompt_int("Please select a zig file update", 0, len(candidates) - 1)
        return candidates[selected_index]

    print("unable to find previous installation in PATH, select one of the following directories to install to:")
    print("--------------------------------------------------------------------------------")
    paths = os.environ["PATH"].split(os.pathsep)
    path_index = 0
    for path in paths:
        print("[{}] '{}'".format(path_index, path))
        path_index += 1
    # selected_index = prompt_int("Please select a path to install zig", 0, len(paths) - 1)
    install_path = "/tmp/zig"
    print("will install zig to '{}'".format(install_path))
    return os.path.join(install_path, basename)

def tar_xz_extract(archive, to):
    # use tar first if it exists, seems to work more often than python's tarfile module
    if find_prog("tar"):
        mkdir(to)
        run(["tar", "-C", to, "-xf", archive])
        return
    import tarfile
    with tarfile.open(archive) as file:
        file.extractall(to)

def zip_extract(archive, to):
    import zipfile
    with zipfile.ZipFile(archive, 'r') as zip:
        zip.extractall(to)

extract_types = (
    (".tar.xz", tar_xz_extract),
    (".zip", zip_extract),
)

def download_and_extract_zig(url, zig_install_path):
    extract_ext = None
    for ext,func in extract_types:
        if url.endswith(ext):
            extract_ext = ext
            extract_func = func
            break
    if not extract_ext:
        sys.exit("Error: don't know how to extract '{}'".format(url))

    archive_basename = os.path.basename(url)
    installed_basename = archive_basename[:-len(extract_ext)]
    print("latest name is '{}'".format(installed_basename))
    versioned_install_path = os.path.join(zig_install_path, installed_basename)
    if os.path.exists(versioned_install_path):
        print("Latest version of zig already downloaded to '{}'".format(versioned_install_path))
    else:
        archive_file = os.path.join(zig_install_path, archive_basename)
        download_if_needed(url, archive_file)

        versioned_install_path_tmp = versioned_install_path + ".extracting"
        if os.path.exists(versioned_install_path_tmp):
            shutil.rmtree(versioned_install_path_tmp)
        print("extracting '{}'...".format(archive_file))
        extract_func(archive_file, versioned_install_path_tmp)
        os.rename(os.path.join(versioned_install_path_tmp, installed_basename), versioned_install_path)
        shutil.rmtree(versioned_install_path_tmp)
        os.remove(archive_file)
    return versioned_install_path


def main():
    args = sys.argv[1:]
    do_clean = False
    for i in range(0, len(args)):
        arg = args[i]
        if arg == "-h" or arg == "--help":
            sys.exit("Usage: zig-build [--clean]")
        elif arg == "--clean":
            do_clean = True
        else:
            sys.exit("Error: unknown command-line argument '{}'".format(arg))

    zig_file_in_path = find_zig_to_update()
    zig_install_path = os.path.dirname(zig_file_in_path)
    if not os.path.exists(zig_install_path):
        sys.exit("Error: zig install path '{}' does not exist".format(zig_install_path))

    if os.name == "nt":
        variant = "x86_64-windows"
    elif sys.platform == "darwin":
        variant = "x86_64-macos"
    else:
        variant = "x86_64-linux"

    url = get_latest_zig_url(variant)
    versioned_install_path = download_and_extract_zig(url, zig_install_path)

    if os.name == "nt":
        #
        # Update batch file to point to new version of zig
        #
        zig_exe = os.path.join(versioned_install_path, "zig.exe")
        if not os.path.exists(zig_exe):
            sys.exit("Error: zig exe '{}' does not exist even after installing zig?".format(zig_exe))
        batch_file_contents = "@{} %*".format(zig_exe)

        update_batch_file = False
        if not os.path.exists(zig_file_in_path):
            print("Creating batch file '{}' with these contents:".format(zig_file_in_path))
            print(batch_file_contents)
            update_batch_file = True
        else:
            with open(zig_file_in_path, "r") as file:
                current_contents = file.read()
            if current_contents == batch_file_contents:
                print("Batch file '{}' already correct:".format(zig_file_in_path))
                print(batch_file_contents)
            else:
                print("-------------------------------------------------")
                print("TODO: prompt to remove old version of zig?")
                print("-------------------------------------------------")
                print("Updating batch file contents.  Current Content:")
                print(current_contents)
                print("New Content:")
                print(batch_file_contents)
                update_batch_file = True

        if update_batch_file:
            with open(zig_file_in_path, "w") as file:
                file.write(batch_file_contents)

    else: # os.name != "nt" (linux)

        link_target = os.path.join(os.path.basename(versioned_install_path), "zig")
        create_symlink = True
        if os.path.exists(zig_file_in_path):
            if os.path.islink(zig_file_in_path):
                existing_link_target = os.readlink(zig_file_in_path)
                if existing_link_target == link_target:
                    print("symlink '{}' is already pointing to '{}'".format(zig_file_in_path, link_target))
                    create_symlink = False
                else:
                    print("symlink '{}' is out-of-date, removing it...".format(zig_file_in_path))
                    os.remove(zig_file_in_path)
            else:
                print("rm '{}'".format(zig_file_in_path))
                os.remove(zig_file_in_path)

        if create_symlink:
            print("ln -s '{}' '{}'".format(link_target, zig_file_in_path))
            os.symlink(link_target, zig_file_in_path)

    if do_clean:
        clean_list = []
        found_latest = False
        for item in glob.glob(os.path.join(zig_install_path, "zig-*")):
            if (item == versioned_install_path):
                found_latest = True
            else:
                clean_list.append(item)

        if not found_latest:
            print("Error: aborting clean because latest compiler '{}' was not found in the install directory:".format(versioned_install_path))
            for d in clean_list:
                print("  {}".format(d))
            sys.exit(1)

        if len(clean_list) == 0:
            print("There are no old compilers to clean")
        else:
            print("Removing {} old compilers...".format(len(clean_list)))
            for d in clean_list:
                logrmtree(d)

main()