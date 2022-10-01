"use strict";

import { exec } from "child_process";

const isWin = process.platform === "win32";

if (isWin) {
    exec('(if exist lib ( rmdir /s /q lib )) && tsc');
} else {
    exec('rm -rf lib && tsc');
}