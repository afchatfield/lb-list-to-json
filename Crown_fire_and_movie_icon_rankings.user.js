// ==UserScript==
// @name         Letterboxd Add Rating Icons
// @namespace    https://github.com/afchatfield
// @version      0.1 (2.1)
// @description  Shows the ranking of each of the top 2000 highest-rated & most popular movies.
// @author       afchatfield
// @match        https://letterboxd.com/film/*
// @icon         https://letterboxd.com/favicon.ico
// @grant        none
// ==/UserScript==

// This userscript is a port of the "Letterboxd Top 2000" Chrome extension by koenhagen

'use strict';
window.addEventListener('load', function (e) {
    let id = parseInt(document.getElementsByClassName("film-poster")[0].parentElement.getAttribute("data-film-id"));
    console.log(id);
    getData(1, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_2000_highest_rated.json')
    getData(2, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/movies_with_100000_or_more_watched.json')
    getData(3, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_5000_all_time.json')
    getData(4, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_250_french_films_by_french_users.json')
    getData(5, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_250_spanish_films_by_spanish_users.json')
    getData(6, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_250_british_films_by_british_users.json')
    getData(7, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_100_german_films_by_german_users.json')
    getData(8, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_100_italian_films_by_italian_users.json')
    getData(9, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_100_scandinavian_films_by_scandinavian_users.json')
    getData(10, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_250_spanish_speaking_films_by_spanish_speaking_users.json')
    getData(11, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_250_japanese_films.json')
    getData(12, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_100_korean_films.json')
    getData(13, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_100_african_films.json')
    getData(14, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_250_romance_films.json')
    getData(15, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_250_comedy_films.json')
    getData(16, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_500_european_films.json')
    getData(17, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_250_south_american_films.json')
    getData(18, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/official-top-250-documentary-films.json')
    getData(19, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_1000_actor_films.json')
    getData(20, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_200_chinese_films.json')
    getData(21, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_250_indian_films.json')
    getData(22, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_100_australian_films.json')
    getData(23, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_100_south_east_asian_films.json')
    getData(24, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_200_middle_eastern_films.json')
    getData(25, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_100_russian_films.json')
    getData(26, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_50_new_zealand_films.json')
    getData(27, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_250_usa_films.json')
    getData(28, id, 'https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_100_canadian_films.json')
    console.log(id);
    getActors('https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Ratings/top_1000_actors.json');

}, false);

let getData = function (type, id, json_link) {
    var parameters;
	fetch(json_link)
		.then(res => res.json())
		.then((out) => {
			out.find(function(item, i){
                //console.log(item, id);
				if (item.Date === id || item.id === id){
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
                    else if (type == 10) {
                        parameters = {
                            list: "/el_duderinno/list/top-250-spanish-speaking-films-as-rated-by/page/",
                            icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/spain_circle.png",
                            size: {height: "14", width: "14"}
                        }
                        addIcon(i+1, parameters);
                    }
                    else if (type == 11) {
                        parameters = {
                            list: "/el_duderinno/list/top-250-japanese-films-as-rated-by-japanese/page/",
                            icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/japan.png",
                            size: {height: "16", width: "16"}
                        }
                        addIcon(i+1, parameters);
                    }
                    else if (type == 12) {
                        parameters = {
                            list: "/el_duderinno/list/letterboxds-top-100-korean-films-as-rated/page/",
                            icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/south-korea.png",
                            size: {height: "16", width: "16"}
                        }
                        addIcon(i+1, parameters);
                    }
                    else if (type == 13) {
                        parameters = {
                            list: "/el_duderinno/list/letterboxds-top-100-african-narrative-films/page/",
                            icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/africa_orange.png",
                            size: {height: "14", width: "14"}
                        }
                        addIcon(i+1, parameters);
                    }
                    else if (type == 14) {
                        parameters = {
                            list: "/el_duderinno/list/top-250-romance-films/page/",
                            icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/romance2.png",
                            size: {height: "14", width: "14"}
                        }
                        addIcon(i+1, parameters);
                    }
                    else if (type == 15) {
                        parameters = {
                            list: "/el_duderinno/list/top-250-comedy-films/page/",
                            icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/comedy1.png",
                            size: {height: "14", width: "14"}
                        }
                        addIcon(i+1, parameters);
                    }
                    else if (type == 16) {
                        parameters = {
                            list: "/el_duderinno/list/top-500-european-films-as-rated-by-european/page/",
                            icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/european-union.png",
                            size: {height: "16", width: "16"}
                        }
                        addIcon(i+1, parameters);
                    }
                    else if (type == 17) {
                        parameters = {
                            list: "/el_duderinno/list/letterboxds-top-250-south-american-films/page/",
                            icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/south-america.png",
                            size: {height: "16", width: "16"}
                        }
                        addIcon(i+1, parameters);
                    }
                    else if (type == 18) {
                        parameters = {
                            list: "/jack/list/official-top-250-documentary-films/page/",
                            icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/book.png",
                            size: {height: "14", width: "16"}
                        }
                        addIcon(i+1, parameters);
                    }
                    else if (type == 19) {
                        parameters = {
                            list: "/el_duderinno/list/top-1000-actors-biggest-film/page/",
                            icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/actor_red.png",
                            size: {height: "12", width: "12", style: 'margin-top: 1px;'}
                        }
                        addIcon(i+1, parameters);
                    }
                    else if (type == 20) {
                        parameters = {
                            list: "/el_duderinno/list/top-100-chinese-films/page/",
                            icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/china.png",
                            size: {height: "16", width: "16"}
                        }
                        addIcon(i+1, parameters);
                    }
                    else if (type == 21) {
                        parameters = {
                            list: "/el_duderinno/list/letterboxds-top-250-indian-films-as-rated/page/",
                            icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/india.png",
                            size: {height: "16", width: "16"}
                        }
                        addIcon(i+1, parameters);
                    }
                    else if (type == 22) {
                        parameters = {
                            list: "/el_duderinno/list/top-100-australian-films-as-rated-by-australian/page/",
                            icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/australia.png",
                            size: {height: "16", width: "16"}
                        }
                        addIcon(i+1, parameters);
                    }
                    else if (type == 23) {
                        parameters = {
                            list: "/el_duderinno/list/top-100-south-east-asian-films/page/",
                            icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/south_east_asia.png",
                            size: {height: "16", width: "16"}
                        }
                        addIcon(i+1, parameters);
                    }
                    else if (type == 24) {
                        parameters = {
                            list: "/el_duderinno/list/top-200-middle-eastern-films/page/",
                            icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/middle_east.png",
                            size: {height: "16", width: "16"}
                        }
                        addIcon(i+1, parameters);
                    }
                    else if (type == 25) {
                        parameters = {
                            list: "/el_duderinno/list/letterboxds-top-100-russian-language-films/page/",
                            icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/russia.png",
                            size: {height: "16", width: "16"}
                        }
                        addIcon(i+1, parameters);
                    }
                    else if (type == 26) {
                        parameters = {
                            list: "/el_duderinno/list/letterboxds-top-50-new-zealand-films-as-rated/page/",
                            icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/new-zealand.png",
                            size: {height: "16", width: "16"}
                        }
                        addIcon(i+1, parameters);
                    }
                    else if (type == 27) {
                        parameters = {
                            list: "/el_duderinno/list/letterboxds-top-250-us-films-as-rated-by/page/",
                            icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/united-states.png",
                            size: {height: "16", width: "16"}
                        }
                        addIcon(i+1, parameters);
                    }
                    else if (type == 28) {
                        parameters = {
                            list: "/el_duderinno/list/letterboxds-top-100-canadian-films-as-rated/page/",
                            icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/canada.png",
                            size: {height: "16", width: "16"}
                        }
                        addIcon(i+1, parameters);
                    }
				}
			});
	});
}

let getActors = function (json_link) {
    const parameters = {
        list: "/el_duderinno/list/top-1000-actors-biggest-film/page/",
        icon: "https://raw.githubusercontent.com/afchatfield/lb-list-to-json/main/Icons/actor_red.png",
        size: {height: "12", width: "12", style: 'margin-top: 1px;'}
    }
    const castAnchors = document.querySelectorAll('#tab-cast .cast-list a');
    const top1000Actors = [];
	fetch(json_link)
		.then(res => res.json())
		.then((out) => {
			out.find(function(item){
                top1000Actors.push(String(item.actor));
			});
            castAnchors.forEach((element, index) => {
                const position = top1000Actors.indexOf(element.textContent.normalize('NFC'));
                if (position >= 0) {
                    //console.log(element, position+1);
                    addActorIcon(element, position+1, parameters);
                };
            });
	});
    // margin-bottom: -1px; margin-left: 3px; margin-right: 3px; display: inline;
}

function createTop250Element(ranking) {
    const div = document.createElement("div");
    div.className = "production-statistic -top250";
    div.setAttribute("aria-label", `№ ${ranking} in the Letterboxd Top 2000`);

    const a = document.createElement("a");
    a.className = "tooltip";
    var page = parseInt(ranking / 100) + 1;
	a.setAttribute("href", "/el_duderinno/list/letterboxds-top-2000-narrative-feature-films-1/page/" + String(page));
    a.setAttribute("data-html", "true");
    a.setAttribute("data-original-title", `№ ${ranking} in the Letterboxd Top 250`);

    const svgNS = "http://www.w3.org/2000/svg";
    const svg = document.createElementNS(svgNS, "svg");
    svg.setAttribute("xmlns", svgNS);
    svg.setAttribute("role", "presentation");
    svg.setAttribute("class", "glyph");
    svg.setAttribute("width", "14");
    svg.setAttribute("height", "11");
    svg.setAttribute("viewBox", "0 0 14 11");

    const path = document.createElementNS(svgNS, "path");
    path.setAttribute("fill", "#000");
    path.setAttribute("d", "M0 2.169c0-.252.126-.48.32-.576.194-.097.417-.043.566.135l2.546 3.056c.05.062.12.095.192.091a.248.248 0 0 0 .188-.108l2.535-3.55A.488.488 0 0 1 6.74 1c.151 0 .295.08.394.218l2.547 3.565c.045.064.109.103.178.108a.236.236 0 0 0 .189-.077l3.091-3.248a.452.452 0 0 1 .556-.098c.185.102.304.323.304.567V10H0V2.169Z");

    svg.appendChild(path);
    a.appendChild(svg);

    const span = document.createElement("span");
    span.className = "label";
    span.textContent = ranking;

    a.appendChild(span);
    div.appendChild(a);

    return div;
}

let addCrown = function (ranking) {
	var div_crown = createTop250Element(ranking);

	new MutationObserver(function(mutations) {
		for (const {addedNodes} of mutations) {
		  for (const node of addedNodes) {
			if (node.nodeType !== Node.ELEMENT_NODE) {
			  continue;
			}
			var icon_div = document.getElementsByClassName("production-statistic-list")[0];
			if (icon_div.getElementsByTagName("div").length > 1) {
				const top250 = document.getElementsByClassName("-top250");
				if(top250.length == 0){
					icon_div.appendChild(div_crown);
				}
				this.disconnect();
			}
		  }
		}
	}).observe(document, {attributes: false, childList: true, characterData: false, subtree:true});
}

let addIcon = function (ranking, parameters) {
    var div_mov = document.createElement("div");
	div_mov.className = "production-statistic"
	var a_mov = document.createElement("a");
    var page = parseInt(ranking / 100) + 1;
	a_mov.setAttribute("href", parameters.list + String(page));
	a_mov.style.fontSize = ".92307692rem"
	let movie = document.createElement("img");
	movie.src = parameters.icon;
    Object.entries(parameters.size).forEach(([key, value]) => {
        movie.setAttribute(key, value);
    });
	movie.style.float = "left";
	movie.style.marginLeft = "-1px";
	movie.style.marginRight = "3px";
	a_mov.appendChild(movie);
	a_mov.appendChild(document.createTextNode(ranking));
	div_mov.appendChild(a_mov);

    new MutationObserver(function(mutations) {
		for (const {addedNodes} of mutations) {
		  for (const node of addedNodes) {
			if (node.nodeType !== Node.ELEMENT_NODE) {
			  continue;
			}
			var icon_div = document.getElementsByClassName("production-statistic-list")[0];
            icon_div.style.flexWrap = 'wrap';
			if (icon_div.getElementsByTagName("div").length > 1) {
				icon_div.appendChild(div_mov);
				this.disconnect();
			}
		  }
		}
	}).observe(document, {attributes: false, childList: true, characterData: false, subtree:true});
}

let addActorIcon = function(element, ranking, parameters) {
	var a_mov = document.createElement("a");
    var page = parseInt(ranking / 100) + 1;
	a_mov.setAttribute("href", parameters.list + String(page));
	a_mov.style.fontSize = ".92307692rem";
	let movie = document.createElement("img");
	movie.src = parameters.icon;
    Object.entries(parameters.size).forEach(([key, value]) => {
        movie.setAttribute(key, value);
    });
	movie.style.display = "inline";
	movie.style.marginBottom = "-1px";
	movie.style.marginLeft = "3px";
    movie.style.marginRight = "3px";
	a_mov.appendChild(movie);
	a_mov.appendChild(document.createTextNode(ranking));

    element.appendChild(a_mov);
}