/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
// this function works exactly the same as UIUtils.formatDuration
function formatDuration(milliseconds) {
    if (milliseconds < 100) {
        return milliseconds + " ms";
    }
    var seconds = milliseconds * 1.0 / 1000;
    if (seconds < 1) {
        return seconds.toFixed(1) + " s";
    }
    if (seconds < 60) {
        return seconds.toFixed(0) + " s";
    }
    var minutes = seconds / 60;
    if (minutes < 10) {
        return minutes.toFixed(1) + " min";
    } else if (minutes < 60) {
        return minutes.toFixed(0) + " min";
    }
    var hours = minutes / 60;
    return hours.toFixed(1) + " h";
}

function formatBytes(bytes, type) {
    if (type !== 'display') return bytes;
    if (bytes == 0) return '0.0 B';
    var k = 1000;
    var dm = 1;
    var sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
    var i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}
