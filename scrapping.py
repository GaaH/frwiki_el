import re
from urllib.parse import quote

_wikinamespaces = [
    "Média",
    "Spécial",
    "Discussion",
    "Utilisateur",
    "Discussion utilisateur",
    "Wikipédia",
    "Discussion Wikipédia",
    "Fichier",
    "Discussion fichier",
    "MediaWiki",
    "Discussion MediaWiki",
    "Modèle",
    "Discussion modèle",
    "Aide",
    "Discussion aide",
    "Catégorie",
    "Discussion catégorie",
    "Portail",
    "Discussion Portail",
    "Projet",
    "Discussion Projet",
    "Référence",
    "Discussion Référence",
    "Module",
    "Discussion module",
    "Gadget",
    "Discussion gadget",
    "Définition de gadget",
    "Discussion définition de gadget",
    "Sujet",
]
_wikinamespaces = [x.lower().strip().replace(" ", "_")for x in _wikinamespaces]
_wikinamespaces = _wikinamespaces + [quote(x).lower() for x in _wikinamespaces]
_wikinamespaces = _wikinamespaces + [x + ":" for x in _wikinamespaces]


def is_title_to_main_ns(title):
    title = title.lower().strip()
    if "#" in title:
        return False

    for x in _wikinamespaces:
        if title.startswith(x):
            return False
    return True


def is_url_to_main_ns(url):
    url = url.lower().strip()
    m = re.search(r"./(.+)", url)
    if m is not None:
        return is_title_to_main_ns(m.group(1))
    return False


def is_internal_link(tag):
    url_prefix = "./"
    is_link = tag.name == "a" and tag.get("href", "").startswith(url_prefix) and tag.has_attr("title")
    if is_link:
        return is_title_to_main_ns(tag["title"]) and is_url_to_main_ns(tag["href"])
    return False


def is_section_to_remove(tag):
    headings = ["notes et références", "notes", "annexes",
                "voir aussi", "bibliographie", "références", "liens externes"]
    return tag.name.startswith("h") and tag.text.lower().strip() in headings


def is_headline(tag):
    # classes = ["bandeau-container" "homonymie" "plainlinks" "hatnote"]
    return tag.has_attr("class") and "bandeau-container" in tag["class"]


def is_infobox(tag):
    # classes = ["bandeau-container" "homonymie" "plainlinks" "hatnote"]
    return tag.has_attr("class") and ("infobox_v3" in tag["class"] or "infobox" in tag["class"])


def is_edit_link(tag):
    return "mw-edit" in tag.attrs.get("class", [])


def is_toc(tag):
    return tag["id"] == "toc"

def is_editsection(tag):
    return tag.name == 'span' and 'mw-editsection' in tag.attrs.get('class', [])