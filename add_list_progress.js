// ==UserScript==
// @name         Letterboxd Lists Progress
// @namespace    https://github.com/frozenpandaman
// @version      0.1 (0.2)
// @description  Displays list progress underneath cover art.
// @author       eli / frozenpandaman | anthony / afchatfield
// @match        https://letterboxd.com/*
// @icon         https://letterboxd.com/favicon.ico
// @grant        GM_addStyle
// @require https://code.jquery.com/jquery-3.6.0.min.js
// ==/UserScript==

GM_addStyle ( `
.list-link, .poster-list-link {
	margin-bottom: 23px !important;
}
.wide-sidebar .list-link, .wide-sidebar .poster-list-link {
	margin-bottom: 31px !important;
}
.list-link.lf-no-margin, .poster-list-link.lf-no-margin, .wide-sidebar .list-link.lf-no-margin, .wide-sidebar .poster-list-link.lf-no-margin, .wide-sidebar .lf-progress-container, .lf-progress-container {
	margin-bottom: 0 !important;
}
.lf-progress-container {
	height: 20px;
	background: #14181c;
	border-top: 1px solid #456;
	position: relative !important;
	border: 1px solid #456 !important;
	border-radius: 3px;
	margin-top: 3px !important;
	padding: 4px !important;
	box-sizing: border-box;
	background-color: #303840;
}
.lf-progress-container:active:after, .lf-progress-container:hover:after {
	display: none !important;
}
.lf-progress-bar {
	position: absolute;
	left: 0;
	top: 0;
	height: 18px;
	background: #40bcf4;
}
.lf-description {
	position: relative;
	color: #fff;
	font-size: 11px;
}
.lf-prgress-counter {
	position: absolute;
	right: 0;
	top: 0;
}
` );

'use strict';

function isListsPage() {
	const hasListLinks = document.querySelector('.list-link') !== null;
	const hasAddButton = document.querySelector('#add-new-button') !== null;
	const isListsPath = window.location.pathname.includes('/lists/');
	const isUserProfile = /\/[^\/]+\/$/.test(window.location.pathname);
	const isFilmPage = /\/film\//.test(window.location.pathname);
	const hasListSections = document.querySelector('section.list') !== null;
	
	return hasListLinks || hasAddButton || isListsPath || isUserProfile || isFilmPage || hasListSections;
}

function passByLists() {
	let listLinks = $(".poster-list-link:not(.lf-checked), .list-link:not(.lf-checked)");
	
	listLinks.each(function() {
		var $self = $(this),
			link = $self.attr("href"),
			$where = $("<div></div>");

		$self.addClass("lf-checked");
		
		$where.load(link + " .progress-panel", function(response, status, xhr) {
			if (status === "error") {
				return false;
			}
			
			if ($where.find(".progress-percentage").length === 0 || $where.find(".progress-percentage").text().length < 1) {
				return false;
			}

			let progressPercentage = $where.find(".progress-percentage").text().trim();
			let progressCount = $where.find(".js-progress-count").text().trim();
			let progressTotal = "";
			
			let progressText = $where.find(".progress-count").text();
			let match = progressText.match(/of\s+([0-9,]+)/i);
			if (match) {
				progressTotal = match[1];
			} else {
				return false;
			}

			let $progress = $("<div class='lf-progress-container'>"
							+ "<div class='lf-progress-bar'></div>"
							+ "<div class='lf-description'>"
								+ "You've watched <span class='lf-progress-count'></span> of <span class='lf-progress-total'></span>"
								+ "<div class='lf-prgress-counter'>"
									+ "<span class='lf-progress-percentage'></span>%"
								+ "</div>"
							+ "</div>"
						+ "</div>");
			
			$progress.find(".lf-progress-percentage").text(progressPercentage);
			$progress.find(".lf-progress-bar").css({"width": progressPercentage + "%"});
			$progress.find(".lf-progress-count").text(progressCount);
			$progress.find(".lf-progress-total").text(progressTotal);

			$self.after($progress).addClass("lf-no-margin");
		});
	});
}

function initializeScript() {
	if (isListsPage()) {
		passByLists();
		setInterval(function() {
			passByLists();
		}, 2000);
	}
}

if (document.readyState === 'loading') {
	document.addEventListener('DOMContentLoaded', initializeScript);
} else {
	initializeScript();
}

window.addEventListener('load', initializeScript, false);