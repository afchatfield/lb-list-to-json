// ==UserScript==
// @name         Letterboxd Top 2000
// @namespace    https://github.com/frozenpandaman
// @version      0.1 (2.1)
// @description  Shows the ranking of each of the top 2000 highest-rated & most popular movies.
// @author       eli / frozenpandaman
// @match        https://letterboxd.com/film/*
// @icon         https://letterboxd.com/favicon.ico
// @grant        none
// ==/UserScript==

// This userscript is a port of the "Letterboxd Top 2000" Chrome extension by koenhagen

'use strict';
window.addEventListener('load', function (e) {
    let id = parseInt(document.getElementsByClassName("film-poster")[0].getAttribute("data-film-id"))

    getData(1, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_2000_highest_rated.json')
    getData(2, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/movies_with_100000_or_more_watched.json')
    getData(3, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_5000_all_time.json')
    getData(4, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_250_french_films_by_french_users.json')
    getData(5, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_250_spanish_films_by_spanish_users.json')
    getData(6, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_250_british_films_by_british_users.json')
    getData(7, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_100_german_films_by_german_users.json')
    getData(8, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_100_italian_films_by_italian_users.json')
    getData(9, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_100_scandinavian_films_by_scandinavian_users.json')
}, false);

let getData = function (type, id, json_link) {
    var parameters;
	fetch(json_link)
		.then(res => res.json())
		.then((out) => {
			out.find(function(item, i){
				if (item.Date === id){
					if (type == 1) {
						 addCrown(i+1);
                    }
					else if (type == 2) {
                        parameters = {
                            list: "/el_duderinno/list/every-movie-with-more-than-100000-watched/page/",
                            icon: "https://raw.githubusercontent.com/frozenpandaman/letterboxd-userscripts/master/top-2000-flame.svg",
                            size: {height: "12", width: "12"}
                        }
                        addIcon(i+1, parameters);
                    }
                    else if (type == 3) {
                        parameters = {
                            list: "/prof_ratigan/list/top-5000-films-of-all-time-calculated/page/",
                            icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/camera_icon.png",
                            size: {height: "16", width: "16"}
                        }
                        addIcon(i+1, parameters);
                    }
                    else if (type == 4) {
                        parameters = {
                            list: "/el_duderinno/list/letterboxds-top-250-highest-rated-french-1/page/",
                            icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/france.png",
                            size: {height: "16", width: "16"}
                        }
                        addIcon(i+1, parameters);
                    }
                    else if (type == 5) {
                        parameters = {
                            list: "/el_duderinno/list/letterboxds-top-250-spanish-speaking-films/page/",
                            icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/spain.png",
                            size: {height: "16", width: "16"}
                        }
                        addIcon(i+1, parameters);
                    }
                    else if (type == 6) {
                        parameters = {
                            list: "/slinkyman/list/letterboxds-top-250-highest-rated-british/page/",
                            icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/united_kingdom.png",
                            size: {height: "16", width: "16"}
                        }
                        addIcon(i+1, parameters);
                    }
                    else if (type == 7) {
                        parameters = {
                            list: "/alexanderh/list/top-100-german-films-of-german-members/page/",
                            icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/germany.png",
                            size: {height: "16", width: "16"}
                        }
                        addIcon(i+1, parameters);
                    }
                    else if (type == 8) {
                        parameters = {
                            list: "/el_duderinno/list/letterboxds-top-100-italian-films-as-rated/page/",
                            icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/italy.png",
                            size: {height: "16", width: "16"}
                        }
                        addIcon(i+1, parameters);
                    }
                    else if (type == 9) {
                        parameters = {
                            list: "/el_duderinno/list/letterboxds-top-100-scandinavian-films-as/page/",
                            icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/scandinavia.png",
                            size: {height: "16", width: "16"}
                        }
                        addIcon(i+1, parameters);
                    }
				}
			});
	});
}

let addCrown = function (ranking) {
	var li_crown = document.createElement("li");
	var a_crown = document.createElement("a");
	li_crown.className = "stat"
    var page = parseInt(ranking / 100) + 1;
	a_crown.setAttribute("href", "/koenie/list/letterboxds-top-2000-narrative-feature-films/page/" + String(page));
	a_crown.className = "has-icon icon-top250 icon-16 tooltip";
	let span = document.createElement("span");
	span.className = "icon";
	a_crown.appendChild(span);
	a_crown.appendChild(document.createTextNode(ranking));
	li_crown.appendChild(a_crown);

	new MutationObserver(function(mutations) {
		for (const {addedNodes} of mutations) {
		  for (const node of addedNodes) {
			if (node.nodeType !== Node.ELEMENT_NODE) {
			  continue;
			}
			var ul = document.getElementsByClassName("film-stats")[0];
			if (ul.getElementsByTagName("li").length > 1) {
				const top250 = document.getElementsByClassName("filmstat-top250");
				if(top250.length == 0){
					ul.appendChild(li_crown);
				}
				this.disconnect();
			}
		  }
		}
	}).observe(document, {attributes: false, childList: true, characterData: false, subtree:true});
}

let addIcon = function (ranking, parameters) {
    var li_mov = document.createElement("li");
	li_mov.className = "stat"
	var a_mov = document.createElement("a");
    var page = parseInt(ranking / 100) + 1;
	a_mov.setAttribute("href", parameters.list + String(page));
	a_mov.style.fontSize = ".92307692rem"
	let movie = document.createElement("img");
	movie.src = parameters.icon;
	movie.setAttribute("height", parameters.size.height);
	movie.setAttribute("width", parameters.size.width);
	movie.style.float = "left"
	movie.style.marginLeft = "-1px"
	movie.style.marginRight = "3px"
	a_mov.appendChild(movie);
	a_mov.appendChild(document.createTextNode(ranking));
	li_mov.appendChild(a_mov);

    new MutationObserver(function(mutations) {
		for (const {addedNodes} of mutations) {
		  for (const node of addedNodes) {
			if (node.nodeType !== Node.ELEMENT_NODE) {
			  continue;
			}
			var ul = document.getElementsByClassName("film-stats")[0];
			if (ul.getElementsByTagName("li").length > 1) {
				ul.appendChild(li_mov);
				this.disconnect();
			}
		  }
		}
	}).observe(document, {attributes: false, childList: true, characterData: false, subtree:true});
}
