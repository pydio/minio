/*
 * MinIO Cloud Storage, (C) 2016-2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"syscall"

	humanize "github.com/dustin/go-humanize"

	"github.com/minio/minio/pkg/disk"
)

const (
	nullVersionID  = "null"
	blockSizeLarge = 2 * humanize.MiByte   // Default r/w block size for larger objects.
	blockSizeSmall = 128 * humanize.KiByte // Default r/w block size for smaller objects.
)

// checkPathLength - returns error if given path name length more than 255
func checkPathLength(pathName string) error {
	// Apple OS X path length is limited to 1016
	if runtime.GOOS == "darwin" && len(pathName) > 1016 {
		return errFileNameTooLong
	}

	// Disallow more than 1024 characters on windows, there
	// are no known name_max limits on Windows.
	if runtime.GOOS == "windows" && len(pathName) > 1024 {
		return errFileNameTooLong
	}

	// On Unix we reject paths if they are just '.', '..' or '/'
	if pathName == "." || pathName == ".." || pathName == slashSeparator {
		return errFileAccessDenied
	}

	// Check each path segment length is > 255 on all Unix
	// platforms, look for this value as NAME_MAX in
	// /usr/include/linux/limits.h
	var count int64
	for _, p := range pathName {
		switch p {
		case '/':
			count = 0 // Reset
		case '\\':
			if runtime.GOOS == globalWindowsOSName {
				count = 0
			}
		default:
			count++
			if count > 255 {
				return errFileNameTooLong
			}
		}
	} // Success.
	return nil
}

func getValidPath(path string) (string, error) {
	if path == "" {
		return path, errInvalidArgument
	}

	var err error
	// Disallow relative paths, figure out absolute paths.
	path, err = filepath.Abs(path)
	if err != nil {
		return path, err
	}

	fi, err := Lstat(path)
	if err != nil && !osIsNotExist(err) {
		return path, err
	}
	if osIsNotExist(err) {
		// Disk not found create it.
		if err = reliableMkdirAll(path, 0777); err != nil {
			return path, err
		}
	}
	if fi != nil && !fi.IsDir() {
		return path, errDiskNotDir
	}

	return path, nil
}

// getDiskInfo returns given disk information.
func getDiskInfo(diskPath string) (di disk.Info, err error) {
	if err = checkPathLength(diskPath); err == nil {
		di, err = disk.GetInfo(diskPath)
	}

	switch {
	case osIsNotExist(err):
		err = errDiskNotFound
	case isSysErrTooLong(err):
		err = errFileNameTooLong
	case isSysErrIO(err):
		err = errFaultyDisk
	}

	return di, err
}

// Function not implemented error
func isSysErrNoSys(err error) bool {
	return errors.Is(err, syscall.ENOSYS)
}

// Not supported error
func isSysErrOpNotSupported(err error) bool {
	return errors.Is(err, syscall.EOPNOTSUPP)
}

// No space left on device error
func isSysErrNoSpace(err error) bool {
	return errors.Is(err, syscall.ENOSPC)
}

// Invalid argument, unsupported flags such as O_DIRECT
func isSysErrInvalidArg(err error) bool {
	return errors.Is(err, syscall.EINVAL)
}

// Input/output error
func isSysErrIO(err error) bool {
	return errors.Is(err, syscall.EIO)
}

// Check if the given error corresponds to EISDIR (is a directory).
func isSysErrIsDir(err error) bool {
	return errors.Is(err, syscall.EISDIR)
}

// Check if the given error corresponds to ENOTDIR (is not a directory).
func isSysErrNotDir(err error) bool {
	return errors.Is(err, syscall.ENOTDIR)
}

// Check if the given error corresponds to the ENAMETOOLONG (name too long).
func isSysErrTooLong(err error) bool {
	return errors.Is(err, syscall.ENAMETOOLONG)
}

// Check if the given error corresponds to the ELOOP (too many symlinks).
func isSysErrTooManySymlinks(err error) bool {
	return errors.Is(err, syscall.ELOOP)
}

// Check if the given error corresponds to ENOTEMPTY for unix,
// EEXIST for solaris variants,
// and ERROR_DIR_NOT_EMPTY for windows (directory not empty).
func isSysErrNotEmpty(err error) bool {
	if errors.Is(err, syscall.ENOTEMPTY) {
		return true
	}
	if errors.Is(err, syscall.EEXIST) && runtime.GOOS == "solaris" {
		return true
	}
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		if runtime.GOOS == globalWindowsOSName {
			var errno syscall.Errno
			if errors.As(pathErr.Err, &errno) {
				// ERROR_DIR_NOT_EMPTY
				return errno == 0x91
			}
		}
	}
	return false
}

// Check if the given error corresponds to the specific ERROR_PATH_NOT_FOUND for windows
func isSysErrPathNotFound(err error) bool {
	if runtime.GOOS != globalWindowsOSName {
		var pathErr *os.PathError
		if errors.As(err, &pathErr) {
			return pathErr.Err == syscall.ENOENT
		}
		return false
	}
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		var errno syscall.Errno
		if errors.As(pathErr.Err, &errno) {
			// ERROR_PATH_NOT_FOUND
			return errno == 0x03
		}
	}
	return false
}

// Check if the given error corresponds to the specific ERROR_INVALID_HANDLE for windows
func isSysErrHandleInvalid(err error) bool {
	if runtime.GOOS != globalWindowsOSName {
		return false
	}
	// Check if err contains ERROR_INVALID_HANDLE errno
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		var errno syscall.Errno
		if errors.As(pathErr.Err, &errno) {
			// ERROR_PATH_NOT_FOUND
			return errno == 0x6
		}
	}
	return false
}

func isSysErrCrossDevice(err error) bool {
	return errors.Is(err, syscall.EXDEV)
}

// Check if given error corresponds to too many open files
func isSysErrTooManyFiles(err error) bool {
	return errors.Is(err, syscall.ENFILE) || errors.Is(err, syscall.EMFILE)
}

func osIsNotExist(err error) bool {
	return errors.Is(err, os.ErrNotExist)
}

func osIsPermission(err error) bool {
	return errors.Is(err, os.ErrPermission)
}

func osIsExist(err error) bool {
	return errors.Is(err, os.ErrExist)
}
