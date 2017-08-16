/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
var uiRoot = "";

function setUIRoot(val) {
    uiRoot = val;
}

function collapseTablePageLoad(name, table){
  if (window.localStorage.getItem(name) == "true") {
    // Set it to false so that the click function can revert it
    window.localStorage.setItem(name, "false");
    collapseTable(name, table);
  }
}

function collapseTable(thisName, table){
    var status = window.localStorage.getItem(thisName) == "true";
    status = !status;

    thisClass = '.' + thisName

    // Expand the list of additional metrics.
    var tableDiv = $(thisClass).parent().find('.' + table);
    $(tableDiv).toggleClass('collapsed');

    // Switch the class of the arrow from open to closed.
    $(thisClass).find('.collapse-table-arrow').toggleClass('arrow-open');
    $(thisClass).find('.collapse-table-arrow').toggleClass('arrow-closed');

    window.localStorage.setItem(thisName, "" + status);
}

// Add a call to collapseTablePageLoad() on each collapsible table
// to remember if it's collapsed on each page reload
$(function() {
  collapseTablePageLoad('collapse-aggregated-metrics','aggregated-metrics');
});