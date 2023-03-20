#!/usr/bin/python3

import sqlparse.sql as sql
from sqlparse import format, parse
from typing import Any
import sys
import os
import traceback
import gzip
import re
import string
import secrets


class Attribute:
    """Trida reprezentujici atribut (vc. pripadneho aliasu a pozadovane hodnoty)"""
    # Typy zavislosti u fiktivnich atributu:
    # Fiktivni atribut je pouzit k predani relevantniho komentare
    CONDITION_COMMENT = "COMMENT"
    # Fiktivni atribut obsahuje referenci (ID) na navazanou tabulku vzniklou pri parsovani "EXISTS SELECT ..."
    CONDITION_EXISTS_SELECT = "EXISTS_SELECT"
    # Fiktivni atribut obsahuje referenci (ID) na jiz existujici navazanou tabulku (typicky subselect)
    CONDITION_DEPENDENCY = "DEPENDENCY"
    # Fiktivni atribut obsahuje jmeno tabulky reprezentujici subselect (vlivem rekurzivniho zpracovavani totiz je na jedno urovni rekurze zjisten pripadny alias a komentar k subselectu a na jine urovni rekurze pak nazev mezi-tabulky pro subselect)
    CONDITION_SUBSELECT_NAME = "SUBSELECT_NAME"
    # Fiktivni atribut s informaci, ze v podminkach byl Placeholder (:PROMENNA)
    CONDITION_PLACEHOLDER_PRESENT = "PLACEHOLDER_PRESENT"
    # Atribut neni kompletni (typicky v dusledku chybneho rozdeleni tokenu na vice casti -- chyby v sqlparse)
    CONDITION_SPLIT_ATTRIBUTE = "SPLIT_ATTRIBUTE"
    # Atribut indikujici skutecnost, ze okolni dva atributy musi byt slouceny zde ulozenou spojkou (potreba typicky v situaci, kdy je operace apod. vracena jako vice tokenu v dusledku pritomnosti "komentar \n" nekde uvnitr)
    CONDITION_SPLIT_ATTRIBUTE_LINK = "SPLIT_ATTRIBUTE_LINK"
    # Atribut typicky obsahujici literal, kde byl SQL kod vlivem chyby v sqlparse rozdelen na vice casti a muze tedy chybet alias
    CONDITION_SPLIT_ATTRIBUTE_ALIAS_MISSING = "SPLIT_ATTRIBUTE_ALIAS_MISSING"
    # Atributy obsahujici pouze jednu cast podminky (leva strana podminky, operator, prava strana podminky; typicky z casti "ON ...", kde jsou v urcitych situacich tokeny vraceny jednotlive)
    CONDITION_SINGLE_ELEMENT = "SINGLE_ELEMENT"
    # Atribut z bloku ve WITH, u ktereho dopredu zname pouze alias ("WITH table(attr_alias_1, attr_alias_2, ...) AS ...")
    CONDITION_TBD = "TO_BE_DETERMINED"

    def __init__(self, name, alias=None, condition=None, comment=None):
        self.set_name(name)
        self.alias = alias
        self.condition = condition
        self.set_comment(comment)

    def set_name(self, name: str) -> None:
        """Nastavi jmeno atributu a zaroven priradi i odpovidajici kratke jmeno (short_name), pokud jmeno neni textovou reprezentaci funkce apod."""
        # pripadna zalomeni radku nahradime mezerami (zde si to muzeme dovolit, jelikoz SQL kod v puvodni podobe je pro pripad potreby k dispozici u tabulek)
        if name != None and len(name) > 0:
            # Zalomeni radku a tabulatory nahradime mezerami
            name = name.replace("\n", " ").replace("\t", " ")
            # Vicenasobne bile znaky taktez nahradime mezerami
            name = re.sub("\\s+", " ", name)
            self.name = name
            if Attribute.is_standard_name(name):
                # Funguje i v pripade, ze tecku nenajdeme (proste vrati name)
                self.short_name = name[(name.rfind(".") + 1):]
            else:
                self.short_name = None
        else:
            self.name = None
            self.short_name = None

    @classmethod
    def is_standard_name(cls, name: str) -> bool:
        """Vrati logickou hodnotu udavajici, zda zadane jmeno splnuje podminky Oracle DB pro pouziti jako identifikator"""
        # V Oracle DB jsou pro nazvy povolene alfanumericke znaky, podtrzitka, dolar a mrizka, pricemz dva posledni uvedene znaky by se pokud mozno nemely uzivat. Kromě techto znaku je nutne povolit i tecku (oddelovac: schema.tabulka.atribut). Zaroven nesmi jit o pouhou sadu cislic, coz by byl ciselny Literal, kde nema smysl nastavovat kratke jmeno.
        return not (re.match("^[a-zA-Z0-9_\\.\\$#]+$", name) == None
                or re.match("^[0-9]+$", name) != None)

    def is_standard_attribute(self) -> bool:
        """Vraci logickou hodnotu udavajici, zda podminka uvedena u atributu je standardni, nebo jde o specialni podminku ze seznamu CONDITION_* v tride Attribute"""
        return (self.name != None
                and self.condition != Attribute.CONDITION_COMMENT
                and self.condition != Attribute.CONDITION_EXISTS_SELECT
                and self.condition != Attribute.CONDITION_DEPENDENCY
                and self.condition != Attribute.CONDITION_SUBSELECT_NAME
                and self.condition != Attribute.CONDITION_PLACEHOLDER_PRESENT
                and self.condition != Attribute.CONDITION_SPLIT_ATTRIBUTE
                and self.condition != Attribute.CONDITION_SPLIT_ATTRIBUTE_LINK
                and self.condition != Attribute.CONDITION_SPLIT_ATTRIBUTE_ALIAS_MISSING
                and self.condition != Attribute.CONDITION_SINGLE_ELEMENT
                and self.condition != Attribute.CONDITION_TBD)

    def set_comment(self, comment: str) -> None:
        """Nastavi komentar u atributu"""
        if comment == None:
            self.comment = None
            return
        # Odstranime vsechny uvodni a koncove znaky oznacujici komentar + bile znaky
        comment = comment.lstrip("-/* \n\t").rstrip("*/ \n\t")
        if len(comment) == 0:
            self.comment = None
            return
        self.comment = comment
    
    def deep_copy(self) -> "Attribute":
        """Vraci "hlubokou" kopii atributu"""
        # Pro zrychleni operace nejprve vytvorime atribut s name == comment == None a tyto nasledne doplnime primym prirazenim (tzn. bez interniho volani metod set_name(...) a set_comment(...))
        attribute = Attribute(name=None, alias=self.alias, condition=self.condition, comment=None)
        attribute.name = self.name
        attribute.short_name = self.short_name
        attribute.comment = self.comment
        return attribute


class Table:
    """Trida reprezentujici tabulku (kazda tabulka ma unikatni ID a jmeno)"""
    # Nejnizsi volne ID
    __next_id__ = 0
    # Mnozina sablon pro automatickou tvorbu nazvu tabulek (klic == sablona, hodnota == aktualni poradove cislo k pouziti pri tvorbe nazvu)
    __next_template_num__ = {}
    # Typy tabulek (nutne pro pozdejsi barevne odliseni v generovanem diagramu/.dia)
    STANDARD_TABLE = 0
    WITH_TABLE = 1
    MAIN_SELECT = 2
    AUX_TABLE = 3
    # Kolekce nalezenych tabulek
    __tables__ = []

    def __init__(self, name=None, name_template=None, attributes=None, conditions=None, comment=None, source_sql=None, table_type=None):
        self.id = Table.__generate_id__()
        if name != None:
            self.name = name
        else:
            # Jmeno nebylo zadane, vygenerujeme ho pomoci sablony
            self.name = Table.__generate_name__(name_template)
        self.statement_aliases = {}
        self.attributes = []
        if attributes != None:
            self.attributes.extend(attributes)
        self.conditions = []
        if conditions != None:
            self.conditions.extend(conditions)
        self.used_bind_vars = []
        # Typ tabulky musime nastavit pred nastavovanim komentare, jeliokz se podle toho ridi, zda rozdelovat ci nerozdelovat komentar na hlavni cast a podkomentar
        if (table_type == None
                or (table_type != Table.STANDARD_TABLE
                and table_type != Table.WITH_TABLE
                and table_type != Table.MAIN_SELECT
                and table_type != Table.AUX_TABLE)):
            table_type = Table.STANDARD_TABLE
        self.table_type = table_type
        self.set_comment(comment)
        self.source_sql = source_sql
        self.linked_to_tables_id = []

    def __str__(self) -> str:
        # Odsazeni pouzivane pri vypisu tabulek
        indent = "    "
        alias_collection = Table.get_all_known_aliases(self.id)
        if len(alias_collection) > 0:
            # Aliasy chceme mit serazene podle abecedy
            alias_collection.sort()
            aliases = f"\n{indent}{indent}".join(alias_collection)
        else:
            aliases = "<žádné>"
        if len(self.attributes) > 0:
            # Textove reprezentace atributu, aliasu a komentaru nejprve ulozime do kolekce
            attribute_collection = []
            for attr in self.attributes:
                if attr.alias != None:
                    alias = f" as {attr.alias}"
                else:
                    alias = ""
                if attr.comment != None and len(attr.comment) > 0:
                    # Komentar muze byt dlouhy --> v takovem pripade vypiseme pouze jeho zacatek
                    attr_comment = f"\n{indent}{indent}{indent}Komentář: \"{Table.__trim_to_length__(attr.comment)}\""
                else:
                    attr_comment = ""
                attribute_collection.append(f"{Table.__trim_to_length__(attr.name)}{alias}{attr_comment}")
            # # Kolekci atributu nebudeme tridit podle abecedy (jednak chceme zachovat poradi atributu a jednak by to zpusobilo problemy v situaci, kdy je vytvarena pomocna tabulka s v kodu natvrdo zadanymi hodnotami)
            # attribute_collection.sort()
            attributes = f"\n{indent}{indent}".join(attribute_collection)
        else:
            attributes = "<žádné>"
        if len(self.conditions) > 0:
            # Zde chceme vypsat podminky z WHERE/ON, takze textove reprezentace budou vc. pripadnych podminek. Postupovat budeme temer stejne jako pri vypisu standardnich atributu.
            attribute_collection = []
            for attr in self.conditions:
                if attr.condition != None:
                    condition = f" {attr.condition}"
                else:
                    condition = ""
                if attr.alias != None:
                    alias = f" as {attr.alias}"
                else:
                    alias = ""
                if attr.comment != None and len(attr.comment) > 0:
                    # Komentar muze byt dlouhy --> v takovem pripade vypiseme pouze jeho zacatek
                    attr_comment = f"\n{indent}{indent}{indent}Komentář: \"{Table.__trim_to_length__(attr.comment)}\""
                else:
                    attr_comment = ""
                attribute_collection.append(f"{Table.__trim_to_length__(attr.name)}{condition}{alias}{attr_comment}")
            # # Ani kolekci podminek nebudeme tridit podle abecedy
            # attribute_collection.sort()
            conditions = f"\n{indent}{indent}".join(attribute_collection)
        else:
            conditions = "<žádné>"
        # Analogicky budeme postupovat u seznamu navazanych tabulek (chceme je mit serazene abecedne podle jmen)
        if len(self.linked_to_tables_id) > 0:
            name_collection = []
            for id in self.linked_to_tables_id:
                for table in Table.__tables__:
                    if table.id == id:
                        name_collection.append(f"{table.name} (ID {id})")
                        break
            names = f"\n{indent}{indent}".join(name_collection)
        else:
            names = "<žádné>"
        comment = Table.__trim_to_length__(self.comment)
        subcomment = Table.__trim_to_length__(self.subcomment)
        source_sql = Table.__trim_to_length__(self.source_sql)
        return f"TABULKA {self.name} (ID {self.id})\n{indent}Všechny známé aliasy:\n{indent}{indent}{aliases}\n{indent}Sloupce:\n{indent}{indent}{attributes}\n{indent}Podmínky (bez uvažování log. spojek):\n{indent}{indent}{conditions}\n{indent}Vazba na tabulky:\n{indent}{indent}{names}\n{indent}Komentář:\n{indent}{indent}\"{comment}\"\n{indent}Podkomentář:\n{indent}{indent}\"{subcomment}\"\n{indent}SQL kód:\n{indent}{indent}\"{source_sql}\""

    @classmethod
    def get_all_known_aliases(cls, table_id: int) -> list:
        """Vraci vsechny zname aliasy tabulky se zadanym ID"""
        if table_id < 0 or table_id > Table.__next_id__ - 1:
            return []
        alias_collection = []
        for table in Table.__tables__:
            if table_id in table.statement_aliases.keys():
                aliases = table.statement_aliases[table_id]
                for a in aliases:
                    if not a in alias_collection:
                        alias_collection.append(a)
        return alias_collection

    @classmethod
    def add_alias(cls, alias_table: "Table", table_id: int, alias: str) -> bool:
        """Ulozi statement alias (slovnik aliasu je ulozen v alias_table) tabulky s ID == table_id. Vraci logickou hodnotu vyjadrujici uspesnost pozadovane operace."""
        if (alias_table == None
                or table_id < 0
                or table_id > Table.__next_id__ - 1
                or alias == None):
            # Neni co, resp. k cemu nastavit...
            return False
        statement_aliases = alias_table.statement_aliases
        if not table_id in statement_aliases:
            statement_aliases[table_id] = [alias]
            return True
        if not alias in statement_aliases[table_id]:
            statement_aliases[table_id].append(alias)
            return True
        # Alias uz je ulozeny z drivejska, takze vratime False
        return False
    
    def add_missing_attributes(self, new_attributes: list) -> None:
        """Prida k aktualni tabulce pripadne chybejici atributy ze zadane kolekce. Namisto pridavani atributu ze zadane kolekce jsou vytvareny jejich "hluboke" kopie a kolekce new_attributes ani neni nijak jinak menena, aby bylo mozne ji pouzit pro naslednou aktualizaci dalsi tabulky (table vs. union_table). Metoda nic nevraci."""
        if new_attributes == None or len(new_attributes) == 0:
            return
        # Pokud je aktualni kolekce atributu prazdna, proste pridamem co jsme dostali
        if len(self.attributes) == 0:
            for new_attr in new_attributes:
                self.attributes.append(new_attr.deep_copy())
            return
        # V akt. kolekci uz neco je a dostali jsme alespon jeden novy atribut. Pokud nova kolekce obsahuje prave jeden "hvezdickovy" ("[table.]*") atribut, nema smysl cokoliv aktualizovat
        if len(new_attributes) == 1 and new_attributes[0].name.endswith("*"):
            return
        # Nyni zkontrolujeme aktualni kolekci -- je tam pouze jeden "hvezdickovy" atribut? Pokud ano, obsah stav. kolekce nahradime obsahem new_attributes (kde uz vime, ze nejde pouze o jeden "hvezdickovy" atribut)
        if len(self.attributes) == 1 and self.attributes[0].name.endswith("*"):
            self.attributes.clear()
            for new_attr in new_attributes:
                self.attributes.append(new_attr.deep_copy())
            return
        # Jsme v situaci, kdy obe kolekce obsahuji alespon jeden specificky atribut (ne "hvezdickovy"). Rozhodovani na zaklade kratkych jmen, resp. aliasu (nutno u literalu) nemusi vubec byt "neprustrelne", ale nic jineho nam nezbyva, protoze atributy mohou byt vlivem chyb v sqlparse vraceny po castech.
        for new_attr in new_attributes:
            attr_missing = True
            for attr in self.attributes:
                if ((attr.short_name != None
                        and new_attr.short_name != None
                        and attr.short_name.lower() == new_attr.short_name.lower())
                        or (attr.alias != None
                        and new_attr.alias != None
                        and attr.alias.lower() == new_attr.alias.lower())):
                    attr_missing = False
                    break
            if attr_missing:
                self.attributes.append(new_attr.deep_copy())
    
    # def mirror_attributes_to_table(self, target_table: "Table") -> None:
    #     """Zrcadli atributy z aktualni tabulky do cilove tabulky (vytvari "hluboke" kopie atributu)"""
    #     if target_table == None:
    #         return
    #     target_table.attributes.clear()
    #     for attribute in self.attributes:
    #         target_table.attributes.append(attribute.deep_copy())

    def uses_bind_vars(self) -> bool:
        """Vraci True, pokud tabulka uziva bindovane promenne"""
        return len(self.used_bind_vars) > 0

    def add_bind_var(self, var: str) -> bool:
        """Prida bindovanou promennou do patricneho seznamu. Vraci logickou hodnotu udavajici uspesnost pozadovane operace."""
        if var == None:
            return False
        var = var.strip()
        if len(var) == 0:
            return False
        if var.startswith(":"):
            var = var[1:]
        if var in self.used_bind_vars:
            return False
        self.used_bind_vars.append(var)
        return True

    def copy_bind_vars_to_table(self, target_table: "Table") -> None:
        """Zkopiruje pouzite bindovane promenne ze seznamu u aktualni tabulky (self.used_bind_vars) do seznamu u cilove tabulky (target_table.used_bind_vars). Metoda nic nevraci."""
        if target_table == None:
            return
        for var in self.used_bind_vars:
            # Zde predpokladame, ze ve zdrojove kolekci jsou jen promenne s "pricetnymi" nazvy, cili muzeme rovnou aktualizovat cilovou kolekci namisto volani add_bind_var(...), kde by znovu probihaly veskere kontroly
            if var in target_table.used_bind_vars:
                continue
            target_table.used_bind_vars.append(var)

    def copy_aliases_to_table(self, target_table: "Table") -> None:
        """Zkopiruje aliasy ze slovniku aktualni tabulky (self.statement_aliases) do slovnku cilove tabulky (target_table.statement_aliases). Metoda nic nevraci."""
        if target_table == None:
            return False
        tables_with_aliases = self.statement_aliases.keys()
        for id in tables_with_aliases:
            if id in target_table.statement_aliases:
                current_aliases = target_table.statement_aliases[id]
                additional_aliases = self.statement_aliases[id]
                for a in additional_aliases:
                    if not a in current_aliases:
                        current_aliases.append(a)

    def set_comment(self, comment: str) -> None:
        """Zadany retezec rozdeli na hlavni komentar a podkomentar a oboji ulozi k tabulce. Metoda nic nevraci."""
        if comment == None:
            self.comment = None
            self.subcomment = None
            return
        # Pokud tabulka neni z WITH, nastavime cely text (patricne orezany + s jednou mezerou nahrazujici kazdou sekvenci bilych znaku) jako hlavni komentar
        if self.table_type != Table.WITH_TABLE:
            self.comment = Table.__lrstrip__(comment, "-/* \n\t")
            self.subcomment = None
            return
        # Nejprve orezeme uvodni a koncove mezery a znacky viceradkoveho komentare
        comment = comment.lstrip(" /*").rstrip(" */")
        split_seq = "--"
        idx = comment.find(split_seq)
        if idx < 0:
            # Oddelovac se v textu nenachazi, cili jsme k ulozeni dostali jen hlavni komentar (opet musime orezat i mezery, ktere mohou typicky byt za uvodnimi pomlckami)
            self.comment = Table.__lrstrip__(comment, "- \n\t")
            self.subcomment = None
            return
        self.comment = Table.__lrstrip__(comment[:idx], "- \n\t")
        # Podkomentar ukladame tak, jak byl v SQL kodu, pouze ze zacatku/konce kazdeho radku odstranime mezery a pomlcky (jako pripadne odrazky byvaji pouzivany hvezdicky, takze si to muzeme dovolit)
        # self.subcomment = comment[idx:].lstrip(" \n\t").rstrip("- \n\t")
        lines = comment[idx:].split("\n")
        for i in range(len(lines)):
            lines[i] = lines[i].lstrip("- \n\t").rstrip("- \n\t")
        text = "\n".join(lines)
        if len(text) == 0:
            self.subcomment = None
        else:
            self.subcomment = text

    @classmethod
    def __lrstrip__(cls, text: str, strip_chars: str) -> str:
        text = text.lstrip(strip_chars).rstrip(strip_chars)
        if len(text) == 0:
            return None
        else:
            return " ".join(text.split())

    @classmethod
    def __trim_to_length__(cls, text: str, max_snippet_length=None) -> str:
        """Zkrati zadany text na max_snippet_length == 50 znaku, prip. vrati puvodni text, pokud byl kratsi. Veskera zalomeni radku, vicenasobne bile znaky apod. jsou zaroven nahrazeny jednotlivymi mezerami."""
        # Potreba v situaci, kdy napr. komentar k tabulce/atributu je None
        if text == None:
            return ""
        text = " ".join(text.split())
        if max_snippet_length == None:
            max_snippet_length = 50
        if len(text) < max_snippet_length:
            return text
        else:
            return text[:(max_snippet_length - 5)] + "[...]"

    @classmethod
    def get_table_by_name(cls, name: str, alias_table: "Table", match_attribute=None, exclude_table_id=-1) -> "Table":
        """Vrati odkaz na tabulku odpovidajici zadanemu jmenu (muze byt i alias), prip. None, pokud v kolekci Table.__tables__ zadna takova tabulka neexistuje. Najdeme-li dve rozdilne tabulky (jednu podle jmena a druhou podle aliasu), je potreba rozhodnout na zaklade jejich atributu pomoci match_attribute. Parametr exclude_table_id slouzi k odfiltrovani aktualne resene tabulky (tato nemuze byt zdrojem informaci sama pro sebe). Porovnavani jmen je case-sensitive!"""
        if name == None:
            return None
        if alias_table == None:
            statement_aliases = {}
        else:
            statement_aliases = alias_table.statement_aliases
        # Dostali jsme v parametru obycejne jmeno (prip. alias), nebo kanonicke jmeno (vc. schematu)?
        bare_name_or_alias = name.rfind(".") < 0
        # Jestlize je zadane jmeno tabulky bez nazvu schematu, ale o tabulce stejneho jmena vc. explicitniho uvedeni nazvu schematu uz vime, jde pravdepodobne (byt ne zcela jiste) o tutez tabulku. Pozor: Oracle nema problem zpracovat i situace typu "SELECT stage.id ... FROM ... INNER JOIN (SELECT stage.stage_id AS id FROM org.stage) AS stage" -- v JOIN/SELECT evidentne referencujeme org.stage, nikoliv alias subselectu, zatimco v hlavnim SELECT referencujeme alias subselectu v JOIN).
        table_via_name = None
        table_via_alias = None
        for table in Table.__tables__:
            # Mame uz nalezeno jak podle jmena, tak podle aliasu?
            if table_via_name != None and table_via_alias != None:
                break
            # Resime tabulku, kterou mame ignorovat?
            if table.id == exclude_table_id:
                continue
            trimmed_name = None
            # Pokud jsme v parametru obdrzeli jmeno bez schematu a nazev tabulky (table.name) mame naopak ulozen vc. schematu, budeme se zadanym jmenem porovnavat i nazev tabulky bez schematu
            if bare_name_or_alias:
                i = table.name.rfind(".")
                if i > 0:
                    trimmed_name = table.name[(i + 1):]
                # Uvnitr podminky jeste rovnou porovname alias
                if table.id in statement_aliases and name in statement_aliases[table.id]:
                    table_via_alias = table
            if name == table.name or name == trimmed_name:
                table_via_name = table
        # Jestlize jsme tabulku nenasli podle nazvu a zaroven nemuze na zaklade match_attribute jit o tabulku z DB (zde by pri korektnim zadani byly v nazvu atributu dve tecky: "schema.tabulka.atribut"), vratime tabulku dle aliasu
        if table_via_name == None:
            return table_via_alias
        # Pokud jsme tabulku nenasli podle aliasu, vratime to, co jsme pripadne nasli podle nazvu. Stejne se zachovame v pripade, ze:
        #   * match_attribute == None (protoze zde by stejne nebylo jak rozhodnout, zda vratit tabulku nalezenou podle jmena, nebo podle aliasu),
        #   * match_attribute != None a zaroven obsahuje vic nez jednu tecku (cili nemuze jit o referenci pres alias tabulky), resp.
        #   * match_attribute != None a zaroven neobsahuje zadnou tecku (de facto nesmyslna situace).
        i = -1
        num_dots = -1
        if match_attribute != None:
            i = match_attribute.rfind(".")
            num_dots = match_attribute.count(".")
        if table_via_alias == None or match_attribute == None or num_dots > 1 or i < 0:
            return table_via_name
        # Z obou hledani (dle nazvu, resp. aliasu) mame dve ruzne tabulky a match_attribute obsahuje jednu tecku, tzn. jde o atribut z tabulky zadane aliasem (prip. nektery alias subselectu koliduje -- bez uvazovani schematu -- s nazvem nektere tabulky v DB). Mezi nalezenymi tabulkami rozhodneme podle match_attribute (= podivame se, u ktere z tabulek se nachazi atribut s potrebnym aliasem, prip. kratkym jmenem). Jako prvni pritom prohledame tabulku nalezenou podle aliasu, protoze
        #   (a) se podle jedine tecky v nazvu atributu odkazujeme spise na ni nez na tabulku z DB (ktera by spravne mela byt zadana vc. schematu, tzn. se dvema teckami v nazvu atributu) a
        #   (b) u takové tabulky mame uplny prehled o vsech atributech.
        match_name = match_attribute[(i + 1):]
        for attribute in table_via_alias.attributes:
            # Preskocime vsechny fiktivni atributy (tzn. s condition != None; toto si muzeme dovolit, jelikoz standardni podminky jsou ukladany do table.conditions)
            if attribute.condition != None:
                continue
            if attribute.short_name == match_name or attribute.alias == match_name:
                return table_via_alias
        # Jestlize jsme neobjevili shodu v tabulce nalezene podle aliasu, musi jit o atribut (snadno i takovy, o kterem explicitne nevime) z tabulky v DB
        return table_via_name

    @classmethod
    def get_table_by_id(cls, id: int) -> "Table":
        """Vrati odkaz na tabulku zadaneho ID, prip. None, pokud v kolekci Table.__tables__ zadna takova tabulka neexistuje"""
        if id == None or id < 0:
            return None
        for table in Table.__tables__:
            if id == table.id:
                return table
        return None

    @classmethod
    def __generate_id__(cls) -> int:
        """Vrati nejnizsi volne celociselne ID, ktere lze priradit aktualne vytvarene tabulce (metoda je volana pouze z kontruktoru)"""
        id = Table.__next_id__
        Table.__next_id__ += 1
        return id

    @classmethod
    def __generate_name__(cls, template: str) -> str:
        """Vytvori jmeno tabulky podle zadane sablony (template). Pokud sablona neni zadana nebo jde pouze o sekvenci mezer, je jako sablona pouzit retezec "table"."""
        if template == None:
            template = "table"
        else:
            # Veskere mezery nahradime pomlckami (predpoklada se, ze v sablone nebude jiny typ bilych znaku)
            template = template.strip().replace(" ", "-")
            if len(template) == 0:
                template = "table"
        # Byla uz zadana sablona drive pouzita? Pokud ano, zjistime aktualni volne poradove cislo, jinak pouzijeme 0.
        try:
            num = Table.__next_template_num__[template]
        except:
            num = 0
        # Nakonec sablonu pridame do mnoziny Table.__next_template_num__ a nastavime nove poradove cislo
        Table.__next_template_num__[template] = num + 1
        return f"{template}-{num}"

    def link_to_table_id(self, id: int) -> bool:
        """Nastavi vazbu aktualni tabulky na tabulku se zadanym ID. Pokud uz vazba existuje (nebo se snazime nastavit vazbu na tabulku samotnou), vrati False, jinak vrati True."""
        if id in self.linked_to_tables_id or id == self.id:
            return False
        self.linked_to_tables_id.append(id)
        return True


# hints_upper = ["MATERIALIZE", "NO_STAR_TRANSFORMATION"]


def is_comment(t: sql.Token) -> bool:
    """Vraci True/false podle toho, zda zadany token je SQL komentarem (tridu nestaci srovnavat jen s sql.Comment!)"""
    if t == None:
        return False
    return (isinstance(t, sql.Comment)
        or t.ttype == sql.T.Comment.Single
        or t.ttype == sql.T.Comment.Multiline)


def split_comment(t: sql.Token) -> list:
    """Vraci zadany komentar rozdeleny podle posledniho vyskytu skupiny deseti nebo vice pomlcek (split_seq; melo by stacit pro dostatecne dobre odliseni casti). Pokud se zmineny oddelovac v hodnote tokenu nenachazi, vrati metoda celou hodnotu jako pocatecni i koncovou cast, jelikoz nelze dopredu rici, kterou z nich bude volajici kod dale pouzivat. Metoda take predpoklada, ze token sam o sobe je nejakou variantou komentare!"""
    split_seq = "----------"
    text = t.value
    idx = text.rfind(split_seq)
    if idx < 0:
        # Oddelovac se v textu nenachazi, cili vratime cely text jako pocatecni i koncovou cast (jen z nej orezeme leaning/trailing pomlcky a mezery)
        text = text.lstrip("- \n\t").rstrip("- \n\t")
        # # Pred vracenim komentare jeste nahradime vicenasobne bile znaky mezerami -- TOTO ZDE NELZE, JELIKOZ PRIPADNY PODKOMENTAR POTREBUJEME TAK, JAK BYL V SQL KODU!
        # text = " ".join(text.split())
        return [text, text]
    # Oddelovac jsme nasli, takze podle nej text rozdelime a kazdou z casti orezeme na zacatku i na konci pomoci .lstrip(...)/.rstrip(...) jako vyse
    leading_portion = text[:idx].lstrip("- \n\t").rstrip("- \n\t")
    trailing_portion = text[(idx + 1 + len(split_seq)):].lstrip("- \n\t").rstrip("- \n\t")
    # # Pred vracenim casti komentare jeste nahradime vicenasobne bile znaky mezerami -- TOTO ZDE NELZE, JELIKOZ PRIPADNY PODKOMENTAR POTREBUJEME TAK, JAK BYL V SQL KODU!
    # leading_portion = " ".join(leading_portion.split())
    # trailing_portion = " ".join(trailing_portion.split())
    return [leading_portion, trailing_portion]


def first_dml_token_is_select(token_list: sql.TokenList) -> bool:
    """Vrati prvni DML nebo Keyword token ze zadaneho seznamu"""
    if token_list == None or len(token_list) == 0:
        return False
    i = 0
    while i < len(token_list) and token_list[i].ttype != sql.T.DML:
        i += 1
    if i == len(token_list):
        return False
    return token_list[i].normalized == "SELECT"


def get_last_nonws_token(t: sql.TokenList) -> sql.Token:
    """Vrati posledni non-whitespace token ze zadaneho seznamu"""
    if t == None or len(t) == 0:
        return None
    i = len(t) - 1
    while i >= 0 and t[i].is_whitespace:
        i -= 1
    if i < 0:
        return None
    return t[i]


def get_name_alias_comment(t: sql.Token) -> tuple:
    """Extrahuje ze zadaneho tokenu jmeno a pripadny alias a komentar (typicke uziti: SELECT ... FROM <token>)"""
    # Struktura: name [ whitespace(s) [ AS whitespace(s) ] alias ]
    # kde "name" muze byt Identifier, prip. Function
    # Pocatecni tokeny v t.tokens jsou soucasti jmena (s pripadnymi oddelovaci/teckami) --> slozky jmena tedy ukladame do kolekce components tak dlouho, nez narazime na bily znak nebo komentar (ev. konec t.tokens)
    # Pokud ovsem t _neni_ Identifier (typicky Operation bez aliasu), muzeme rovnou vratit name s tim, ze alias je None a komentar zjistime podobne jako nize
    if not isinstance(t, sql.Identifier):
        last_nonws_token = get_last_nonws_token(t.tokens)
        if is_comment(last_nonws_token):
            # Je-li zde komentar, zajiman nas obecne jeho uvodni cast (pred serii pomlcek)
            return t.normalized, None, split_comment(last_nonws_token)[0]
        return t.normalized, None, None
    i = 0
    components = []
    # Nejprve musime obejit BUG "connect_by_root". Pokud jsme narazili na danou situaci, mame [connect_by_root] [ws] [name AS alias]
    if t.value.lower().startswith("connect_by_root"):
        components.append(t.tokens[0].normalized)
        components.append(" ")  # Musime ulozit i mezeru za "connect_by_root"!
        # Nyni preskocime vsechny nasledujici bile znaky (mezery, \n apod.)
        i = 1
        while i < len(t.tokens) and t.tokens[i].is_whitespace:
            i += 1
        # Do promenne t ulozime nasledujici token, kde je jmeno + pripadny alias a komentar, resetujeme index i a pokracujeme standardnim zpusobem
        t = t.tokens[i]
        i = 0
    while (i < len(t.tokens)
            and not t.tokens[i].is_whitespace
            and not is_comment(t.tokens[i])):
        components.append(t.tokens[i].normalized)
        i += 1
    name = "".join(components)
    # Nyni preskocime vsechno, co je bilym znakem nebo klicovym slovem "AS" (na komentar ted narazit nemuzeme -- bud jsme ho museli najit v predchozim kroku, nebo bude az za aliasem)
    while (i < len(t.tokens) and (t.tokens[i].is_whitespace
            or (t.tokens[i].ttype == sql.T.Keyword and t.tokens[i].normalized == "AS"))):
        i += 1
    alias = None
    components = []
    # V tuto chvili jsme v na zacatku aliasu, takze do components opět pridavame vse tak dlouho, nez narazime na bily znak nebo komentar (ev. konec t.tokens)
    while (i < len(t.tokens)
            and not t.tokens[i].is_whitespace
            and not is_comment(t.tokens[i])):
        components.append(t.tokens[i].value)
        i += 1
    # Alias sloucime z jednotlivych komponent do jednoho retezce
    if len(components) > 0:
        alias = "".join(components)
    # Nakonec jeste overime typ posledniho tokenu v t.tokens -- jde-li o komentar, vratime ho spolu se jmenem a aliasem (pripadne bile znaky za komentarem uz sqlparse nevraci jako soucast tokenu t). Z komentare neni potreba odstranovat leading/trailing whitespaces, jelikoz toto je provedeno v kontruktoru.
    last_nonws_token = get_last_nonws_token(t.tokens)
    if is_comment(last_nonws_token):
        # Je-li zde komentar, zajiman nas obecne jeho uvodni cast (pred serii pomlcek)
        return name, alias, split_comment(last_nonws_token)[0]
    return name, alias, None


def process_comparison(t: sql.Comparison) -> Attribute:
    """Vraci atribut vc. pozadovane hodnoty (typicke uziti: ... JOIN ... ON <token>)"""
    # Dohledani zavislosti udelame ihned, aby bylo mozne korektne ulozit informace o pripadnych subselectech
    attributes = process_identifier_list_or_function(t, only_save_dependencies=True)
    subselect_names, attributes = get_subselect_names(attributes)
    # Pocatecni tokeny v t.tokens jsou soucasti jmena atributu (s pripadnymi oddelovaci/teckami) --> tyto ukladame do components. Ulozeni provedeme bez ohledu na pocet subselectu (len(subselect_names)), protoze stejne musime postupnym prochazenim t.tokens zjistit pouzity operator.
    components = []
    j = 0
    # Posledni cast podminky je nutna v pripade, ze pred operatorem neni mezera. POZOR: "...ttype != sql.T.Comparison" NENI TOTEZ JAKO "not isinstance(..., sql.Comparison)"!
    name_is_literal = True 
    while j < len(t.tokens) and not t.tokens[j].is_whitespace and t.tokens[j].ttype != sql.T.Comparison:
        name_is_literal = name_is_literal and t.tokens[j].ttype in sql.T.Literal
        components.append(t.tokens[j].value)
        j += 1
    name = "".join(components)
    # Ted preskocime vse az po misto, kde se nachazi Comparison (operator)
    while j < len(t.tokens) and t.tokens[j].ttype != sql.T.Comparison:
        j += 1
    # Hodnotu tokenu s operatorem si ulozime v uppercase verzi, jinak by napr. "in" bylo malymi pismeny
    operator = t.tokens[j].normalized.upper()
    # Teprve nyni se podivame, kolik subselectu jsme nasli pri zjistovani zavislosti. pokud byly dva, musel byt jeden na kazde strane operatoru, cili dalsi prochazeni t.tokens je uz zbytecne. Jinak rozhodneme pozdeji name a value.
    parse_value = True
    if len(subselect_names) == 1:
        # Nasli jsme jen jeden subselect
        if Attribute.is_standard_name(name) or name_is_literal:
            # Promenna name obsahuje pouze znaky povolene pro identifikatory, tzn. subselect (s mezerami, zavorkami atd.) musi byt napravo od operatoru
            value = get_op_prefix(subselect_names[0])
            parse_value = False
        else:
            name = get_op_prefix(subselect_names[0])
    elif len(subselect_names) == 2:
        name = get_op_prefix(subselect_names[0])
        value = get_op_prefix(subselect_names[1])
        parse_value = False
    if parse_value:
        # Preskocime veskere bile znaky za operatorem...
        j += 1
        while j < len(t.tokens) and t.tokens[j].is_whitespace:
            j += 1
        # ... a do components si ulozime veskere tokeny (predstavujici pozadovanou hodnotu atributu) az po prvni bily znak, prip. komentar
        components = []
        while j < len(t.tokens) and not t.tokens[j].is_whitespace and not is_comment(t.tokens[j]):
            components.append(t.tokens[j].value)
            j += 1
        value = "".join(components)
    # Nakonec jeste overime typ posledniho tokenu v t.tokens -- jde-li o komentar, vratime ho spolu se jmenem a pozadovanou hodnotou (pripadne bile znaky za komentarem uz sqlparse nevraci jako soucast tokenu t). Z komentare neni potreba odstranovat leading/trailing whitespaces, jelikoz toto je provedeno  vkontruktoru.
    last_nonws_token = get_last_nonws_token(t.tokens)
    if is_comment(last_nonws_token):
        # Z komentare nas zajima pouze cast po pripadnyou delsi serii pomlcek
        attributes.append(Attribute(name=name, condition=f"{operator} {value}", comment=split_comment(last_nonws_token)[0]))
    else:
        attributes.append(Attribute(name=name, condition=f"{operator} {value}"))
    return attributes


def get_subselect_names(attributes: list) -> list:
    """Vraci seznam subselectu ze zadaneho seznamu atributu vc. aktualizovane kolekce atributu bez patricnych fiktivnich atributu"""
    if attributes == None:
        return [], None
    subselect_names = []
    j = 0
    while j < len(attributes):
        if attributes[j].condition == Attribute.CONDITION_SUBSELECT_NAME:
            subselect_names.append(attributes[j].comment)
            attributes.pop(j)
            continue
        j += 1
    return subselect_names, attributes


def get_op_prefix(subselect_names: list) -> str:
    """Vraci prefix s nazvy subselectu (ulozeny v seznamu) pro vlozeni do nazvu atributu nebo podminky"""
    if subselect_names == None or len(subselect_names) == 0:
        return None
    if isinstance(subselect_names, str):
        # Pokud jsme do metody predali obycejny retezec namisto seznamu retezcu, sloucime ho s "Op(" a ")" standardnim zpusobem
        return "Op(" + subselect_names + ")"
    return "Op(" + ", ".join(subselect_names) + ")"


def get_attribute_conditions(t: sql.Token) -> list:
    """Vraci seznam atributu vc. jejich pozadovanych hodnot. Zadany token muze byt obycejnym porovnanim (Comparison), sekvenci sub-tokenu urcujicich napr. rozmezi hodnot ("rok BETWEEN 2010 AND 2020" apod.), prip. "EXISTS ( ... )"."""

    # TODO: zatim ignoruje logicke spojky mezi podminkami -- je toto ale nutne resit?

    attributes = []
    # Postup se lisi podle toho, zda sqlparse vratil jednoduche srovnani (Comparison), sekvenci tokenu (napr. pro urceni rozmezi hodnot), nebo je toto navic v zavorce (Parenthesis) ci jako soucast [ WHERE | ON ] EXISTS.
    if isinstance(t, sql.Comparison):
        # Token je obycejnym srovnanim, takze staci do kolekce attributes pridat navratovou hodnotu process_comparison(...) (nelze vratit primo tuto navratovou hodnotu, tzn. objekt typu Attribute, protoze typ navratove hodnoty se pozdeji poziva k rozliseni, jak presne s takovou hodnotou nalozit). Zaroven musime namisto .append() pouzit .extend(), jelikoz je vlivem dohledavani zavislosti vracen seznam atributu, nikoliv pouze jeden atribut!
        attributes.extend(process_comparison(t))
        return attributes
    if t.ttype == sql.T.Comparison:  # != sql.Comparison!
        # BUG: pokud je v SQL kodu "JOIN ... ON ... AND name comment NOT IN ( SELECT ... )", vraci toto sqlparse jako oddelene tokeny ([name comment] [NOT IN] [(SELECT ... )]). Na tuto sekvenci pritom narazime primo v process_statement(...), tzn. tokeny jsou pak posilany oddelene do zdejsi metody. Situaci tedy musime nejprve detekovat a potom postupne vracet zpet nekompletni atributy (condition = Attribute.CONDITION_SINGLE_ELEMENT). Pokud by za operatorem byl komentar, byl by sqlparse vracen jako dalsi token, tzn. nema smysl toto zde resit.
        attributes.append(Attribute(name=t.normalized, condition=Attribute.CONDITION_SINGLE_ELEMENT))
        return attributes
    if isinstance(t, sql.Identifier):
        # Projdeme t.tokens, pricemz dopredu vime, ze kolekce attributes bude ve vysledku obsahovat jediny standardni atribut
        # Nejprve dohledame pripadne zavislosti pomoci process_identifier_list_or_function(..., only_save_dependencies=True)
        dep_attr = process_identifier_list_or_function(t, only_save_dependencies=True)
        subselect_names, dep_attr = get_subselect_names(dep_attr)
        attributes.extend(dep_attr)
        comment = ""
        # BUG: viz popis "JOIN ... ON ... AND name comment NOT IN ( SELECT ... )" vyse
        name_only = False
        try:
            i = 0
            token = t.token_first(skip_ws=True, skip_cm=True)
            while token != None and (token.ttype == sql.T.Name or token.value == "."):
                (i, token) = t.token_next(i, skip_ws=True, skip_cm=True)
            if token == None:
                # Dosli jsme na konec t.tokens, tzn. jsou tam pouze subtokeny typu Name a tecky (plus pripadne bile znaky a komentare, ktere zde nejsou podstatne)
                name_only = True
        except:
            pass
        # Dostali jsme ke zpracovani pouze identifikator s pripadnym komentarem?
        if name_only:
            try:
                last_nonws_subtoken = get_last_nonws_token(t.tokens)
                if is_comment(last_nonws_subtoken):
                    comment = split_comment(last_nonws_subtoken)[0]
            except:
                pass
            if len(subselect_names) > 0:
                name = get_op_prefix(subselect_names)
            else:
                name = t.normalized
            attributes.append(Attribute(name=name, condition=Attribute.CONDITION_SINGLE_ELEMENT, comment=comment))
            return attributes
        # Podminka je kompletni, muzeme ji nacist a ulozit celou jako standardni atribut (bez ohledu na pripadne subselecty -- stejne musime projit subtokeny kvuli pripadnych komentaru)
        comment = ""
        i = 0
        token = t.token_first(skip_ws=True, skip_cm=False)
        name = token.normalized  # Ulozime si .normalized, tzn. bez pripadneho komentare
        (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
        operator = token.normalized
        (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
        components = []
        while token != None:
            if token.ttype == sql.T.Keyword:
                components.append(token.normalized)
            elif token.ttype in sql.T.Literal:
                components.append(token.value)
            elif is_comment(token):
                # Z komentare nas zajima pouze cast po pripadnyou delsi serii pomlcek
                comment = split_comment(token)[0]
            else:
                break
            (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
        value = " ".join(components)
        # Tvar atributu doladime podle pripadnych subselectu
        if len(subselect_names) > 0:
            attributes.append(Attribute(name=get_op_prefix(subselect_names), comment=comment))
        else:
            attributes.append(Attribute(name=name, condition=f"{operator} {value}", comment=comment))

        # TODO: co kdyz zde bude subselect? Muze to vubec nastat?

        return attributes
    if isinstance(t, sql.Parenthesis) and first_dml_token_is_select(t.tokens):
        # Zpracovavame "( SELECT ... ) [komentar]", tzn. musi jit pouze o cast posdminky (--> condition=Attribute.CONDITION_SINGLE_ELEMENT)
        dep_attr = process_identifier_list_or_function(t, only_save_dependencies=True)
        subselect_names, dep_attr = get_subselect_names(dep_attr)
        attributes.extend(dep_attr)
        comment = ""
        try:
            last_nonws_subtoken = get_last_nonws_token(t.tokens)
            if is_comment(last_nonws_subtoken):
                comment = split_comment(last_nonws_subtoken)[0]
        except:
            pass
        # Zde musi v subselect_names byt alespon jeden prvek!
        attributes.append(Attribute(name=get_op_prefix(subselect_names), condition=Attribute.CONDITION_SINGLE_ELEMENT, comment=comment))
        return attributes
    if isinstance(t, sql.Parenthesis) or isinstance(t, sql.Where):
        # Projdeme t.tokens a postupne rekurzivne zpracujeme kazdy z patricnych sub-tokenu. Zaroven potrebujeme referenci na posledni token v t.tokens, abychom pripadne mohli predat relevantni komentar zpet do hlavni casti kodu. Zohlednit musime i pripadne klicove slovo WHERE.
        comment = ""
        last_nonws_token = get_last_nonws_token(t.tokens)
        comment_before = ""  # Pro pripad, ze by bylo nutne vytvorit mezi-tabulku bez predchoziho vyskytu komentare
        split_operation = False  # Pro pripad, ze je podminka rozdelena na vice radku a pred nekterym z \n je komentar
        prev_token_normalized = None  # Pro pripad, ze je podminka rozdelena na vice tokenu vlivem pritomnosti extra zavorek
        # Prvni token preskocime (jde o oteviraci zavorku, resp. WHERE)
        (i, token) = t.token_next(0, skip_ws=True, skip_cm=False)
        while token != None:
            # Nejprve vyresime BUG, kdy vlivem pridani komentare do operace rozdelene na vice radku (napr. "WHERE value > a -- komentar \n + b") dojde k rozdeleni podminky na vice tokenu
            if split_operation:
                attributes[-1].condition += " " + token.value
                split_operation = False
                dep_attr = process_identifier_list_or_function(token, only_save_dependencies=True)
                subselect_names, dep_attr = get_subselect_names(dep_attr)
                if len(subselect_names) > 0:
                    attributes[-1].condition = get_op_prefix(subselect_names)  # + ": " + attributes[-1].condition
                attributes.extend(dep_attr)
            elif token.ttype in sql.T.Operator and len(attributes) > 0:
                attributes[-1].condition += "\n " + token.value
                split_operation = True
            elif (prev_token_normalized != None
                    and (token.ttype in sql.T.Operator
                    or (token.ttype == sql.T.Keyword
                    and token.normalized != "AND"
                    and token.normalized != "OR"))):
                # BUG: je-li v SQL kodu neco s extra zavorkami, resp. pokud jde o operaci a zaroven argument v podmince s "textovymi" operatory, sqlparse vraci takovou zavorku jako separatni token!
                # Jako prvni vec extrahujeme z attributes pripadna jmena subselectu a nahradime jimi budouci jmeno atributu (tzn. do prev_token_value)
                subselect_names, attributes = get_subselect_names(attributes)
                if len(subselect_names) > 0:
                    prev_token_normalized = get_op_prefix(subselect_names)
                # Ulozime si pro referenci udaj o tom, kolik "hodnotovych" tokenu musime nacist (operator --> 1, klicove slovo --> 3)
                if token.ttype in sql.T.Operator:
                    n_tokens_to_store = 1
                else:
                    n_tokens_to_store = 3
                # Predchozi token byl zavorkou a nyni nasleduje vlastni podminka
                components = []
                # Nacteme prvni cast podminky (operator, resp. "NOT BETWEEN" apod.)
                while token != None and (token.ttype in sql.T.Operator or token.ttype == sql.T.Keyword):
                    components.append(token.normalized)
                    (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
                # Dalsi tokeny uz nutne musi obsahovat podminku, resp. pripadny komentar
                n_read_tokens = 0
                while token != None and n_read_tokens < n_tokens_to_store:
                    if is_comment(token):
                        # Zde je komentar obecne uvaden v tokenu nasledujicim po specifikaci podminky, cili nas zajima jeho pocatecni cast
                        comment = split_comment(token)[0]
                    else:
                        # Cokoliv jineho si ulozime (protoze bile znaky preskakujeme pri hledani tokenu a Punctuation apod. tady syntakticky nedava smysl). Ukladame vsak .normalized, cimz dojde k orezani pripadnych internich komentaru.
                        dep_attr = process_identifier_list_or_function(token, only_save_dependencies=True)
                        attributes.extend(dep_attr)
                        # Nasli jsme v podmince subselect?
                        subselect_names, dep_attr = get_subselect_names(dep_attr)
                        attributes.extend(dep_attr)
                        if len(subselect_names) > 0:
                            components.append(get_op_prefix(subselect_names))
                        else:
                            components.append(token.normalized)
                        # Komentar mohl take byt primo soucasti tokenu (jako posledni non-whitespace subtoken). Pokud tomu tak bylo, aktualizujeme promennou comment. Toto ale musime obalit try-except, jelikoz ne kazdy token ma atribut tokens!
                        try:
                            last_nonws_subtoken = get_last_nonws_token(token.tokens)
                            if is_comment(last_nonws_subtoken):
                                comment = split_comment(last_nonws_subtoken)[0]
                        except:
                            pass
                        n_read_tokens += 1
                    (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
                # Jeste musime snizit index (i), abychom nepreskocili aktualni token, ktery muze byt podstatny. Toto ale lze udelat jen v pripade, ze i != None (nenasleduje-li zadny dalsi token, je metodou token_next(...) vraceno (None, None)!)
                if i != None:
                    i -= 1
                condition = " ".join(components)
                attributes.append(Attribute(name=prev_token_normalized, condition=condition, comment=comment))
            elif token.normalized == "START":
                # Podminka obsahuje i cast "[START] [WITH] [...]" --> preskocime nasledujici dva tokeny ("WITH ...") a u posledniho z nich dohledame pripadne zavislosti; pozor: zde preskakujeme i komentare
                (i, token) = t.token_next(i, skip_ws=True, skip_cm=True)  # "[WITH]"
                (i, token) = t.token_next(i, skip_ws=True, skip_cm=True)  # "[...]"
                attributes.extend(process_identifier_list_or_function(token, only_save_dependencies=True))
            elif token.normalized == "CONNECT":
                # Podminka obsahuje i cast "[CONNECT] [BY] [[NOCYCLE]]" --> preskocime na nasledujici token ("BY"), ale pripadne (volitelne!) klicove slovo "NOCYCLE" vyresime oddelene; pozor: zde preskakujeme i komentare
                (i, token) = t.token_next(i, skip_ws=True, skip_cm=True)  # "[BY]"
            elif token.normalized == "NOCYCLE":
                # Podminka obsahuje i cast "[CONNECT] [BY] [NOCYCLE]" --> nedelame nic, skok na dalsi token probehne na konci cyklu
                pass
            elif token.normalized == "PRIOR":
                # Podminka obsahuje i cast "PRIOR ..." --> preskocime na nasledujici token a dohledame u nej pripadne zavislosti; pozor: zde preskakujeme i komentare
                (i, token) = t.token_next(i, skip_ws=True, skip_cm=True)
                attributes.extend(process_identifier_list_or_function(token, only_save_dependencies=True))
            elif isinstance(token, sql.Parenthesis) and first_dml_token_is_select(token.tokens):
                # Subselect je soucasti podminky (napr. "( SELECT ... ) BETWEEN ..."). Dohledame tedy zavislosti, na konci cyklu nastavime prev_token_value (protoze token je typu Parenthesis) a v ukladani zbytku tokenu budeme pokracovat v dalsi iteraci (budou pritom take z attributes extrahovna jmena subselectu).
                attributes.extend(process_identifier_list_or_function(token, only_save_dependencies=True))
            elif is_comment(token):
                # Z komentare nas zajima pouze cast za pripadnou delsi serii pomlcek
                comment_before = split_comment(token)[1]
                # Ukladat budeme jen neprazdny komentar (nikoliv vysledny komentar po zpracovani "-- \n" apod.)
                if token == last_nonws_token:
                    # Zde jsme narazili na komentar k mezi-tabulce (napr. "JOIN ... ON ( ... ) komentar"), prip. komentar k nasledujicimu bloku v SQL kodu. Pridame fiktivni atribut (name == alias == None, condition == Attribute.CONDITION_COMMENT, comment != None), ze ktereho pak bude komentar extrahovan.
                    attributes.append(Attribute(name=None, alias=None, condition=Attribute.CONDITION_COMMENT, comment=comment_before))
                    return attributes
                if len(attributes) > 0:
                    # Jde o komentar k poslednimu nalezenemu atributu
                    attributes[-1].set_comment(token.value.strip())
            elif token.ttype == sql.T.Keyword and token.normalized == "EXISTS":
                # Typicky pripad: "JOIN ... ON ( attr = value AND EXISTS ( ... ) AND ... )"
                (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
                while token != None:
                    if is_comment(token):
                        # Pripadny komentar si ulozime, jelikoz by se tykal nasledujici mezi-tabulky "EXISTS ( SELECT ... )"
                        comment_before = split_comment(token)[1]
                    elif isinstance(token, sql.Parenthesis):
                        # Nasli jsme zavorku se SELECT, k cemuz je nutne vytvorit patricnou mezi-tabulku
                        exists_table = Table(name_template="exists-select", comment=comment_before, table_type=Table.AUX_TABLE)
                        Table.__tables__.append(exists_table)
                        # Nove vytvorenou mezi-tabulku jeste musime svazat s hlavni tabulkou, na kterou tady ale nemame referenci. Pridame proto fiktivni atribut (name == alias == None, condition == Attribute.CONDITION_EXISTS_SELECT, comment == ID exists_table), ze ktereho bude patricny udaj v hlavnim kodu extrahovan
                        attributes.append(Attribute(name=None, alias=None, condition=Attribute.CONDITION_EXISTS_SELECT, comment=str(exists_table.id)))
                        # Krome ID mezitabulky musime do hlavniho kodu predat take informaci o podmince. Hned jako dalsi atribut tedy pridame jmennou referenci na exists_table vc. pripadneho komentare a tento pak v hlavnim kodu ulozime mezi podminky.
                        attributes.append(Attribute(name=f"<{exists_table.name}>", alias=None, condition=None, comment=comment_before))
                        # Zavorku nyni zpracujeme jako standardni statement s tim, ze parametrem predame referenci na vytvorenou mezi-tabulku (veskere pripadne zavislosti budou dohledany rekurzivne v process_statement(...))
                        process_statement(token, exists_table)
                        break
                    (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
            elif isinstance(token, sql.Function) and token.tokens[0].value.upper() == "EXISTS":
                # Situace podobna pripadu vyse, avsak zde SQL kod obsahuje "EXISTS( ... )" (tzn. mezi "EXISTS" a "(" neni mezera) --> token je nyni typu Function a zpracovani je nutne provest malinko jinak!
                exists_table = Table(name_template="exists-select", comment=comment_before, table_type=Table.AUX_TABLE)
                Table.__tables__.append(exists_table)
                # Nove vytvorenou mezi-tabulku jeste musime svazat s hlavni tabulkou --> pridame fiktivni atribut (name == alias == None, condition == Attribute.CONDITION_EXISTS_SELECT, comment == ID exists_table)
                attributes.append(Attribute(name=None, alias=None, condition=Attribute.CONDITION_EXISTS_SELECT, comment=str(exists_table.id)))
                # Krome ID mezitabulky predame do hlavniho kodu take informaci o podmince (= jmennou referenci na exists_table vc. pripadneho komentare)
                attributes.append(Attribute(name=f"<{exists_table.name}>", alias=None, condition=None, comment=comment_before))
                # Zavorku -- ulozenou v token.tokens[1] -- nyni zpracujeme jako standardni statement s tim, ze parametrem predame referenci na vytvorenou mezi-tabulku (veskere pripadne zavislosti budou dohledany rekurzivne v process_statement(...))
                process_statement(token.tokens[1], exists_table)
            elif (isinstance(token, sql.Identifier)
                    or isinstance(token, sql.Function)
                    or token.ttype in sql.T.Literal
                    or token.normalized == "NULL"  # Porovnavat budeme pomoci .normalized (oreze pripadne komentare)
                    or token.ttype == sql.T.Name.Placeholder):
                # Nasledujici tokeny v t.tokens budeme prochazet tak dlouho, nez ziskame jednu kompletni podminku. Toto nelze resit rekurzivne opetovnym volanim get_attribute_conditions(...), protoze tokeny musime prochazet na stavajici urovni (token \in t.tokens), nikoliv o uroven nize (token.tokens)
                # Pripadne zavislosti je nutne dohledavat prubezne, jelikoz postupne nacitame dalsi tokeny!
                comment = ""
                dep_attr = process_identifier_list_or_function(token, only_save_dependencies=True)
                subselect_names, dep_attr = get_subselect_names(dep_attr)
                attributes.extend(dep_attr)
                if len(subselect_names) > 0:
                    name = get_op_prefix(subselect_names)
                else:
                    name = token.value
                (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
                # Pripadne komentare pred operatorem apod. si ulozime
                while token != None and is_comment(token):
                    comment = split_comment(token)[0]
                    (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
                if isinstance(token, sql.Comparison):
                    # Viz BUG popsany nize, zde lze narazit na situaci (a)
                    # token.tokens[0] obsahuje zbytek leve strany podminky, pak je nutne postupovat v token.tokens analogicky kodu nize. Aktualizovat potom budeme posledni standardni (name != None) atribut z rekurzivne zpracovaneho tokenu.
                    attr = get_attribute_conditions(token)
                    for j in range(len(attr) - 1, -1, -1):
                        if attr[j].name != None:
                            break
                    attr[j].name = name + " " + attr[j].name
                    attributes.extend(attr)
                else:
                    operator = token.normalized
                    (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
                    if operator == "IS":
                        dep_attr = process_identifier_list_or_function(token, only_save_dependencies=True)
                        subselect_names, dep_attr = get_subselect_names(dep_attr)
                        attributes.extend(dep_attr)
                        if len(subselect_names) > 0:
                            value = get_op_prefix(subselect_names)
                        else:
                            value = token.normalized
                        (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
                        if token != None and is_comment(token):
                            # Zde je komentar obecne uvaden v tokenu nasledujicim po specifikaci atributu, cili nas zajima jeho pocatecni cast
                            comment = split_comment(token)[0]
                    else:
                        components = []
                        while token != None and len(components) < 3:
                            if is_comment(token):
                                # Zde je komentar obecne uvaden v tokenu nasledujicim po specifikaci atributu, cili nas zajima jeho pocatecni cast
                                comment = split_comment(token)[0]
                            else:
                                # Cokoliv jineho si ulozime (protoze bile znaky preskakujeme pri hledani tokenu a Punctuation apod. tady syntakticky nedava smysl). Ukladame vsak .normalized, cimz dojde k orezani pripadnych internich komentaru.
                                dep_attr = process_identifier_list_or_function(token, only_save_dependencies=True)
                                # Nasli jsme v podmince subselect?
                                subselect_names, dep_attr = get_subselect_names(dep_attr)
                                attributes.extend(dep_attr)
                                if len(subselect_names) > 0:
                                    components.append(get_op_prefix(subselect_names))
                                else:
                                    components.append(token.normalized)
                                # Komentar mohl take byt primo soucasti tokenu (jako posledni non-whitespace subtoken). Pokud tomu tak bylo, aktualizujeme promennou comment. Toto ale musime obalit try-except, jelikoz ne kazdy token ma atribut tokens!
                                try:
                                    last_nonws_subtoken = get_last_nonws_token(token.tokens)
                                    if is_comment(last_nonws_subtoken):
                                        comment = split_comment(last_nonws_subtoken)[0]
                                except:
                                    pass
                            (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
                        # Jeste musime snizit index (i), abychom nepreskocili aktualni token, ktery muze byt podstatny. Toto ale lze udelat jen v pripade, ze i != None (nenasleduje-li zadny dalsi token, je metodou token_next(...) vraceno (None, None)!)
                        if i != None:
                            i -= 1
                        value = " ".join(components)
                    attributes.append(Attribute(name=name, condition=f"{operator} {value}", comment=comment))
            elif isinstance(token, sql.Operation):
                # Operace je vracena jako separatni token. Zde pouze dohledame zavislosti; ulozeni podminky (a pripadna extrakce jmen subselectu) probehne nasledne.
                attributes.extend(process_identifier_list_or_function(token, only_save_dependencies=True))
            elif token.ttype in sql.T.Literal and len(attributes) > 0:
                # BUG v sqlparse: Pokud podminka obsahuje napr. artimetickou operaci a patricny operator neni od cisla oddelen mezerou, jsou tokeny vraceny spatne. Priklady:
                #   (a) "tab.col -1 = tab2.col" --> 2 tokeny: "tab.col" (Identifier), "-1 = tab2.col" (Comparison)
                #   (b) "tab.col = tab2.col +1" --> 2 tokeny: "tab.col = tab2.col" (Comparison), "+1" (Literal)
                # Zde jsme narazili na pripad (b), kde staci u posledniho atributu aktualizovat podminku.
                attributes[-1].condition += " " + token.value
            elif token.ttype != sql.T.Keyword and token.ttype != sql.T.Punctuation:
                # Jde o obycejny atribut (prip. jejich vycet)
                attributes.extend(get_attribute_conditions(token))
            # Pokud jsme resili zavorku, ulozime si pro jistotu aktualni token.value, jelikoz toto muze bt potreba, pokud bychom v nasledujicim tokenu narazili na klicove slovo/a NOT BETWEEN/IN apod.
            if isinstance(token, sql.Parenthesis) or isinstance(token, sql.Operation):
                prev_token_normalized = token.normalized
            else:
                prev_token_normalized = None
            # Nakonec musime prejit na dalsi token (zde neni nutne kontrolovat, i != None (tzn. zda jsme uz na konci), protoze v takovem pripade je rovnou vraceno (None, None) a cyklus tedy standardne opustime podminkou ve WHILE)
            (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
        return attributes


def process_with_element(t, comment_before="") -> str:
    """Kompletne zpracuje kod bloku ve WITH (vc. vytvoreni odpovidajici tabulky) a vrati komentar, ktery byl na konci t.tokens (protoze ten se dost mozna vztahuje ke kodu, ktery nasleduje po aktualne zpracovavanem bloku)."""
    # Pro osetreni BUGu popsaneho nize v kodu muze comment_before byt None. V takovem pripade byl WITH blok umele rozdelen na dva tokeny a nyni teprve zpracovavame ten druhy. Promennou statement_sql na zacatku inicializujeme pomoci None, z cehoz nize pozname, zda pokracovat s vytvarenim tabulky atd. (tokeny v podobe carky, bileho znaku apod. chceme preskocit a jednoduse vratit comment_before tak, jak jsme ho dostali).
    statement = None
    comment_after = comment_before
    if isinstance(comment_before, tuple):
        # Zde zpracovavame zbytek umele rozdeleneho tokenu (jen zavorka plus pripadny komentar), tzn. name comment_before  == (comment_before, name, aliases, zacatek_sql_kodu)
        name = comment_before[1]
        aliases = comment_before[2]
        source_sql = comment_before[3] + t.value
        comment_before = comment_before[0]
        # Token t je rovnou typu Parenthesis (pripadny komentar za zavorkou je poslednim tokenem v t.tokens)
        statement = t
        last_nonws_token = get_last_nonws_token(t.tokens)
        if is_comment(last_nonws_token):
            # Jde o komentar k zavorce, cili nas zajima primarne to, co nasleduje az po serii pomlcek
            comment_after = split_comment(last_nonws_token)[1]
        else:
            comment_after = ""
    else:
        # Standardni struktura tokenu nasledujicich po klicovem slove WITH: Identifier [ [ whitespace(s) ] Punctuation [ whitespace(s) ] Identifier [ ... ] ]
        # --> Zde zpracovavame pouze tokeny typu Identifier.
        if isinstance(t, sql.Identifier):
            # Struktura t.tokens: name whitespace(s) [ column_aliases whitespace(s) ] [ AS whitespace(s) ] ( SELECT ... ) [ whitespace(s) comment ]
            # Pokud je uveden jen nazev tabulky, je prvni token typu Name. Jsou-li za nazvem tabulky v zavorkach uvedeny aliasy atributu, je prvni token typu Function.
            aliases = []
            if t.tokens[0].ttype == sql.T.Name:
                name = t.tokens[0].value
            else:
                name = t.tokens[0].tokens[0].value
                # Po nacteni jmena preskocime vse az do zavorky (Parenthesis), ve ktere jsou aliasy atributu
                i = 1
                while i < len(t.tokens[0].tokens) and not isinstance(t.tokens[0].tokens[i], sql.Parenthesis):
                    i += 1
                # Je-li v zavorce jeden alias, pak nas zajima pouze sub-token typu Identifier. Pokud je v zavorce vice aliasu, jsou vraceny jako jeden sub-token typu IdentifierList, ktery musime dale projit a nacist z nej vsechny Identifiery.
                for par_token in t.tokens[0].tokens[i].tokens:
                    if isinstance(par_token, sql.Identifier):
                        aliases.append(par_token.value)
                    elif isinstance(par_token, sql.IdentifierList):
                        for alias_token in par_token.tokens:
                            if isinstance(alias_token, sql.Identifier):
                                aliases.append(alias_token.value)
            # V tuto chvili mame zjisteny nazev docasne tabulky i pripadne aliasy atributu. Od druheho tokenu v t.tokens tedy hledame tak dlouho, nez najdeme zavorku (ve ktere se nachazi "SELECT ...").
            i = 1
            while i < len(t.tokens) and not isinstance(t.tokens[i], sql.Parenthesis):
                i += 1
            # Zpracovani zavorky se SELECT vsak zatim preskocime -- nejprve si ulozime pripadny komentar z konce t-tokens a vyrobime tabulku reprezentujici zpracovavany blok, jejiz atributy budou doplneny/aktualizovany pozdeji (az se k nim dostaneme pri prochazeni SQL kodu)
            last_nonws_token = get_last_nonws_token(t.tokens)
            if is_comment(last_nonws_token):
                # Jde o komentar k zavorce, cili nas zajima primarne to, co nasleduje az po serii pomlcek
                comment_after = split_comment(last_nonws_token)[1]
            else:
                comment_after = ""
            # BUG: Je-li v kodu "name [AS] komentar \n (...)", vrati sqlparse jako prvni token pouze "name [AS] komentar" a samotna zavorka nasleduje az v dalsim tokenu. Toto osetrime kontrolou indexu i (== len(t.tokens) znamena, ze zavorka nebyla soucasti tokenu) a okamzitym vracenim ntice s veskerymi doszd zjistenymi udaji. Puvodni comment_before a odpovidajici cast SQL kodu totiz potrebujeme zachovat a zaroven nemuzeme vracet seznam retezcu, protoze tomu by v pythonu odpovidal i standardni retezec. Na zaklade datoveho typu comment_before pak v dalsim volani teto metody pozname, ze k nove predanemu tokenu je nutne pristupovat jinak.
            source_sql = t.value
            if i == len(t.tokens):
                return (comment_before, name, aliases, source_sql)
            statement = t.tokens[i]
    # Nyni uz pokracujeme identicky (az na podminku tykajici se statement_sql) bez ohledu na to, zda jsme zpracovavali obvykly WITH blok, nebo blok umele rozdeleny do dvou tokenu
    if statement != None:
        table = Table(name=name, comment=comment_before, source_sql=source_sql, table_type=Table.WITH_TABLE)
        Table.__tables__.append(table)
        if len(aliases) > 0:
            # Zname uz aliasy atributu (byly v zavorce za nazvem tabulky), ale nic vic k atributum tabulky nevime. Pouze tedy nastavime parametr, na zaklade ktereho pak v hlavni casti kodu (process_statement(...)) budou k atributum doplneny zbyle udaje. Jmena atributu (stejne jako condition) budou pro poradek -- at nejsou None -- docasne Attribute.CONDITION_TBD.
            known_attribute_aliases = True
            for a in aliases:
                table.attributes.append(Attribute(name=Attribute.CONDITION_TBD, condition=Attribute.CONDITION_TBD, alias=a))
        else:
            known_attribute_aliases = False
        # Nakonec doresime zavorku, odkaz na jiz vytvorenou tabulku predame stejne jako parametr ohledne (ne)znalosti aliasu atributu
        process_statement(statement, table, known_attribute_aliases)
    return comment_after


def process_identifier_list_or_function(t: sql.Token, only_save_dependencies=False) -> list:
    """Zpracuje token typu Identifier nebo Function a vrati odpovidajici atribut. Je-li pro popsani atributu potreba mezi-tabulka (napr. pokud je misto obycejneho atributu "( SELECT ... )" nebo "( CASE ... )"), vrati krome odpovidajiciho atributu i fiktivni atribut s udajem pro svazani nadrazene tabulky s nove vytvorenou mezi-tabulkou (name == alias == condition == None, comment == ID mezi-tabulky). Parametr only_save_dependencies urcuje, zda chceme ukladat nalezene atributy, nebo nas zajimaji jen pripadne zavislosti na jinych tabulkach."""
    if (t.is_whitespace
            or t.ttype == sql.T.Punctuation
            or t.ttype == sql.T.Keyword
            or (only_save_dependencies and t.ttype in sql.T.Literal)):
        # Whitespace ani Punctuation nas nezajimaji, Keyword taky nema smysl parsovat. Naopak Literal budeme parsovat v pripade, ze neukladame pouze zavislosti.
        return []
    if t.ttype in sql.T.Operator:
        # BUG: v dusledku pritomnosti "komentar \n" uvnitr operace apod. muze byt patricna cast kodu rozdelena na vice tokenu. Pridame tedy fiktivni atribut, ktery -- pokud pak bude "z obou stran" mit standardni atributy -- nakonec pouzijeme pro slouceni umele rozdelenych casti.
        if only_save_dependencies:
            return []
        return [Attribute(name=None, alias=None, condition=Attribute.CONDITION_SPLIT_ATTRIBUTE_LINK, comment=t.value)]
    # Pokud jde o Placeholder, vratime fiktivni atribut (name == alias == None, condition == Attribute.CONDITION_PLACEHOLDER_PRESENT, comment == nazev placeholderu), podle ktereho pak bude mozne pridat placeholder do seznamu u tabulky, resp. potazmo v generovanem diagramu barevne odlisit patricnou tabulku a uvest u ni seznam bindovanych promennych
    if t.ttype == sql.T.Name.Placeholder:
        attributes = []
        attributes.append(Attribute(name=None, alias=None, condition=Attribute.CONDITION_PLACEHOLDER_PRESENT, comment=t.value))
        # Pripadny komentar nikdy neni soucasti placeholderu (BUG: sqlparse takovy komentar zcela preskoci!)
        if not only_save_dependencies:
            attributes.append(Attribute(name=t.value))
        return attributes
    # POZOR: sqlparse neumi WITHIN GROUP(...) (napr. "SELECT LISTAGG(pt.typ_program,', ') WITHIN GROUP(ORDER BY pt.typ_program) AS programy FROM ...") --> BUG report ( https://github.com/andialbrecht/sqlparse/issues/700 ). Podobne je nekdy vracena funkce COUNT (a nejspis i jine funkce) -- nazev fce je vracen jako klicove slovo na konci Identifier (za carkou; resp. posledniho Identifieru v IdentifierList) a zavorka s parametry pak jako zacatek naledujiciho tokenu.
    # Bugy vyse prozatim obejdeme tak, ze pri zpracovavani vzdy overime posledni subtoken (Identifier WITHIN (vraceno jako Identifier), resp. Keyword s nazvem funkce -- pokud ano, je temer jiste, ze jde o zminenou situaci a posledni nalezeny atribut pak bude nekompletni (--> nastavime u nej condition na Attribute.CONDITION_SPLIT_ATTRIBUTE, podle cehoz pak v hlavnim kodu pozname, ze tento je nekompletni). Takovy nekompletni atribut pritom muze vzdy byt uveden pouze jako posledni ve vracenem seznamu atributu.
    split_attr_link = None
    last_nonws_token = None
    # Literal nema .tokens, takze ho musime vyloucit...
    if not (only_save_dependencies or t.ttype in sql.T.Literal) and t.tokens != None:
        last_nonws_token = get_last_nonws_token(t.tokens)
        if isinstance(last_nonws_token, sql.Identifier) and last_nonws_token.value.lower() == "within":
            # Musi byt s mezerami na zacatku/konci, aby naopak funkce (COUNT apod.) mohly byt bez mezer mezi nazvem a zavorkou
            split_attr_link = " WITHIN GROUP "
        elif last_nonws_token.ttype == sql.T.Keyword:
            split_attr_link = ""
        elif last_nonws_token.ttype in sql.T.Literal:
            # Pokud je posledni non-whitespace subtoken typu Keyword nebo Literal, je pravdepodobne, ze vlivem BUGu v sqlparse doslo k umelemu rozdeleni vyctu atributu na vice tokenu (u Literalu napr. v situaci "SELECT ..., Literal alias", tzn. kdyz mezi Literalem a aliasem NENI "AS").
            split_attr_link = "ALIAS_MISSING"
    attributes = []
    if isinstance(t, sql.IdentifierList):
        # Jednotlive tokeny projdeme a zpracujeme. Parametr only_save_dependencies pritom musime nastavit podle jeho aktualni hodnoty. Pokud jsme si jeste nezjistili posledni non-whitespace token, je treba toto jeste udelat (bude potreba nize pro vyreseni BUGu).
        if last_nonws_token == None:
            last_nonws_token = get_last_nonws_token(t.tokens)
        for token in t.tokens:
            # BUG (Literal): pokud jsme narazili na carku a posledni nalezeny atribut je s potencialne chybejicim aliasem, je zrejme, ze alias nebyl uveden. Podminku tedy z posledniho atributu odstranime.
            if (token.ttype == sql.T.Punctuation
                    and len(attributes) > 0
                    and attributes[-1].condition == Attribute.CONDITION_SPLIT_ATTRIBUTE_ALIAS_MISSING):
                attributes[-1].condition = None
                # Muzeme rovnou pokracovat ve zpracovavani dalsiho tokenu
                continue
            attributes.extend(process_identifier_list_or_function(token, only_save_dependencies=only_save_dependencies))
            # BUG: posledni token je Identifier (napr. pokud v SQL kodu je "NVL (...)", tzn. s mezerou mezi nazvem funkce a zavorkou). Ulozime tedy nazev funkce; ze jde o rozdeleny atribut, bude nastaveno na konci metody na zaklade promenne split_attr_link == "" (prirazeno vyse).
            if token == last_nonws_token and token.ttype == sql.T.Keyword:
                attributes.append(Attribute(name=token.value))
    elif isinstance(t, sql.Parenthesis):
        # Nasli jsme zavorku, jejiz obsah je potreba projit. Zde musime zkontrolovat druhy non-whitespace token z first-token (jelikoz ten prvni je oteviraci zavorkou) a vec doresit dle situace.
        i = 1
        while i < len(t.tokens) and t.tokens[i].is_whitespace:
            i += 1
        first_token = t.tokens[i]
        if first_token.ttype == sql.T.DML and first_token.normalized == "SELECT":
            # Namisto bezneho atributu pracujeme se zavorkou, ve ktere je dalsi SELECT. S ohledem na moznou delku SELECTu vezmeme jako nazev atributu pouze nazev odpovidajici mezi-tabulky a obsah zavorky zpracujeme jako separatni statement (podobne jako napr. JOIN). Nakonec nastavime zavislosti tabulek. Pripadny komentar k tabulce, ktery uz ale zde nemame k dispozici, nastavime az dodatecne v hlavnim kodu podle atributu reprezentujiciho tuto tabulku.
            table = Table(name_template="select", table_type=Table.AUX_TABLE)
            Table.__tables__.append(table)
            # Sub-token se SELECT je hned jako prvni, neni potreba hledat ho iterovanim pres token.tokens
            process_statement(t, table)
            # Nakonec je nutne zaridit nastaveni zavislosti nadrazene tabulky. Na tu ale zde nemame k dispozici odkaz. ID nove mezi-tabulky tedy predame jako fiktivni atribut (name == alias == None, condition == Attribute.CONDITION_DEPENDENCY, comment == ID) a zavislost (prip. zavislosti, nebot jich muze byt vice) pak doresime v hlavnim kodu.
            attributes.append(Attribute(name=None, alias=None, condition=Attribute.CONDITION_DEPENDENCY, comment=str(table.id)))
            # Nazev tabulky jeste musime (bez ohledu na only_save_dependencies!) predat do volajiciho kodu, aby bylo mozne spraven nastavit jmeno atributu reprezentujiciho zpracovavany SELECT (chceme pouze "<select-N>", nikoliv kompletni "(SELECT ... FROM ...)"). Toto zaridime pridanim fiktivniho atributu (name == alias == None, condition == Attribute.CONDITION_SUBSELECT_NAME, comment = jmeno odp. tabulky), ktery pak bude ve volajicim kodu nalezite aktualizovan udaji o pripadne maliasu subselectu a komentari.
            attributes.append(Attribute(name=None, alias=None, condition=Attribute.CONDITION_SUBSELECT_NAME, comment=f"<{table.name}>"))
        elif first_token.ttype == sql.T.Keyword and first_token.normalized == "CASE":
            # V pripade CASE pouze dohledame zavislosti
            for token in t.tokens:
                attributes.extend(process_identifier_list_or_function(token, only_save_dependencies=True))
        else:
            # U vseho ostatniho rekurzivne projdeme tokeny vc. ukladani nazvu atributu atd.
            for token in t.tokens:
                attributes.extend(process_identifier_list_or_function(token, only_save_dependencies=only_save_dependencies))
    elif isinstance(t, sql.Case) or isinstance(t, sql.Comparison):
        for token in t.tokens:
            attributes.extend(process_identifier_list_or_function(token, only_save_dependencies=only_save_dependencies))
    elif isinstance(t, sql.Identifier):
        # Jmeno a pripadny alias zjistime pomoci get_name_alias_comment(...)
        name, alias, comment = get_name_alias_comment(t)
        # Pokud je prvni non-whitepace token z t.tokens (vzdy na indexu 0) typu Name, je v t opravdu jen jmeno, prip. take alias a komentar. SYSDATE je vracen jako Name, byt jde o vestavenou funkci (--> toto preskocime toutez podminkou). V ostatnich pripadech musime prvni subtoken rekurzivne analyzovat a ulozit pouze zavislosti (samotny token bude ulozen hned v podmince nize)
        if t.tokens[0].ttype != sql.T.Name:  # and t.normalized.lower() != "sysdate":
            # Pri rekurzivnim zpracovani kodu zde nevime, zda v t.tokens[0] nebyl napr. dalsi SELECT, k cemuz ale je potreba ulozit atribut s jinym nazvem ("<select-N>" namisto "(SELECT ... FROM ...)"). Vime vsak, ze pokud k takove situaci doslo, je poslednim atributem v attributes fiktivni atribut (kontrolovat budeme pro rychlost pouze podle condition == Attribute.CONDITION_SUBSELECT_NAME, comment = jmeno odp. tabulky). Subselectu ale muze byt vice (!), cili je nutne zkontrolovat vsechny prvky attributes, pripadne fiktivni atributy odebereme a jmeno vysledneho atributu upravime tak, aby odpovidalo resene situaci. Pak uz jen obvyklym zpusobem pridame novy atribut, je-li to potreba na zaklade only_save_dependencies.
            dep_attr = process_identifier_list_or_function(t.tokens[0], only_save_dependencies=True)
            subselect_names, dep_attr = get_subselect_names(dep_attr)
            attributes.extend(dep_attr)
            if len(subselect_names) > 0:
                # Postup nutny pro zjisteni, zda slo o samostatny subselect, resp. byl jeden ci vice subselectu soucasti operace/funkce/..., by nebyl uplne trivialni a na tomto miste asi ani neni uplne potreba toto resit. Do jmena tedy pouze na zacatek pridame, ze jde o _nejakou_ operaci a s jakymi subselecty.
                name = get_op_prefix(subselect_names)  # + ": " + name
        if not only_save_dependencies:
            attributes.append(Attribute(name=name, alias=alias, comment=comment))
    elif isinstance(t, sql.Operation):
        # Rekurzivne dohledame zavislosti, nicmene pro zamezeni zacykleni je nutne zpracovavat zde kazdy subtoken zvlast.
        for token in t.tokens:
            attributes.extend(process_identifier_list_or_function(token, only_save_dependencies=True))
        # Podobne jako u funkce potrebujeme ulozit atribut a nasledne dohledat zavislosti (pritom zpracovavame vsechny subtokeny, zatimco u funkce pracujeme s druhym subtokenem!)
        if not only_save_dependencies:
            # Jmena subselectu muzeme extrahovat pouze zde, kdyz uz jsou dohledane zavislosti a ukladame patricny standardni atribut
            name, alias, comment = get_name_alias_comment(t)
            subselect_names, attributes = get_subselect_names(attributes)
            if len(subselect_names) > 0:
                name = get_op_prefix(subselect_names)
            attributes.append(Attribute(name=name, alias=alias, comment=comment))
    elif isinstance(t, sql.Function):
        # Nejprve rekurzivne projdeme veskere argumenty funkce a dohledame pripadne tabulkove zavislosti (--> only_save_dependencies=True). Resit pritom budeme az druhy token v t.tokens, nebot v tom prvnim (t.tokens[0]) je ulozen nazev funkce.
        attributes.extend(process_identifier_list_or_function(t.tokens[1], only_save_dependencies=True))
        # Zda pujde o rozdeleny atribut, se dozvime az pozdeji, kdy pripadne v process_statement(...) narazime na samostatny token (Keyword) WITHIN. Toto se ale doresi v hlavnim kodu.
        # Pokud je v SQL kodu "<funkce> <komentar> WITHIN GROUP (ORDER BY ...)", vrati sqlparse tokeny:
        #   * <funkce> <komentar>
        #   * WITHIN
        #   * GROUP
        #   * (ORDER BY ...)
        # pripadne analogii tokenu vyse, neni-li za GROUP mezera. Jde tedy o dalsi specialni situaci, kterou musime doresit rucne. Ma-li samotny atribut byt take ulozen, zacneme tim, ze si zjistime jmeno atd.
        if not only_save_dependencies:
            name, alias, comment = get_name_alias_comment(t)
            attributes.append(Attribute(name=name, alias=alias, comment=comment))
    elif ((t.ttype in sql.T.Literal or t.ttype == sql.T.Keyword)
            and not only_save_dependencies):
        # Nasli jsme literal (typicky v situaci, kdy je ve WITH definovana pomocna tabulka s konkretnimi -- v SQL kodu zadanymi -- hodnotami) a tento si zaroven na zaklade parametru only_save_dependencies potrebujeme ulozit
        attributes.append(Attribute(name=t.normalized))
    # Nakonec jeste nastavime condition na Attribute.CONDITION_SPLIT_ATTRIBUTE a comment na patricny spojovaci retezec, pokud je posledni atribut nekompletni (je-li condition jiz nastaveno, pak hodnotu nesmime prepsat!)
    if split_attr_link != None and attributes[-1].condition == None:
        # condition aktualizujeme pouze v pripade, ze jsme opravdu alias nenasli (napr. u "SELECT x AS result ...") ho totiz najdeme a nastavenim condition = Attribute.CONDITION_SPLIT_ATTRIBUTE_ALIAS_MISSING bychom pak prisli i o pripadny zjisteny komentar!)
        if split_attr_link == "ALIAS_MISSING":
            if attributes[-1].alias == None:
                attributes[-1].condition = Attribute.CONDITION_SPLIT_ATTRIBUTE_ALIAS_MISSING
        else:
            attributes[-1].condition = Attribute.CONDITION_SPLIT_ATTRIBUTE
            # U fiktivniho atributu musime komentar s ohledem na pritomnost mezer priradit primo, nikoliv pomoci set_comment(...)!
            attributes[-1].comment = split_attr_link
    return attributes


def process_token(t, alias_table: Table, context=None, comment_before="") -> Any:
    """Zpracuje zadany token; typ vraceneho objektu zavisi na tom, jakeho typu token je a v jakem kontextu se nachazi (napr. SELECT <token> ... vrati odkaz na vytvorenou tabulku apod.). Informace o zdrojovych tabulkach jsou vzdy vraceny jako seznam."""
    if context != None and "select" in context:
        # Nejprve vyresime situaci, kdy je v SQL kodu hint ("+ MATERIALIZE" apod.), nebo byly tokeny umele rozdeleny na vice casti vlivem pritomnosti "komentar \n" uvnitr operace apod. Vratime proto fiktivni atribut, ktery -- pokud se ukaze, ze byl nesmyslny -- zahodime.
        if t.ttype == sql.T.Operator:
            return [Attribute(name=None, alias=None, condition=Attribute.CONDITION_SPLIT_ATTRIBUTE_LINK, comment=t.value)]
        if t.ttype == sql.T.Name.Placeholder:
            return process_identifier_list_or_function(t, only_save_dependencies=False)
        # Token je v kontextu lib. mutace SELECT (std., UNION SELECT, ...). Pokud je token typu Parenthesis, je potreba vytvorit odpovidajici (mezi-)tabulku a zavorku pak zpracovat jako samostatny SQL statement. Do process_statement(...) pritom musime predat odkaz na novou tabulku, aby bylo mozne spravne priradit nalezene atributy atd. Krome toho muze token reprezentovat i "( SELECT ...) AS ..." nebo "( CASE ... ) AS ..." ve vyctu atributu.
        if isinstance(t, sql.Parenthesis):
            # Zde resime UNION SELECT nebo "SELECT ... FROM ( SELECT ... )"; nemuze jit o "( SELECT ...) AS ..." nebo "( CASE ... ) AS ..." ve vyctu atributu, protoze tam musi byt alias (a takovy token tedy je typu Identifier[List])
            table = Table(name_template=context, comment=comment_before, table_type=Table.AUX_TABLE)
            Table.__tables__.append(table)
            process_statement(t, table)
            return [table]
        attributes = []
        # Je-li token typu Identifier, IdentifierList, Function, prip. Wildcard, jde o obycejny atribut ci seznam atributu. Metoda pak podle toho vrati seznam s jednim ci vicero atributy. I kdybychom ale zpracovavali napr. "SELECT (CASE ...)", tento by musel byt v zavorce, za kterou by musel byt alias, takze by toto opet bylo vraceno jako Identifier.
        if (isinstance(t, sql.IdentifierList)
                or isinstance(t, sql.Identifier)
                or isinstance(t, sql.Function)
                or isinstance(t, sql.Operation)):
            attr = process_identifier_list_or_function(t, only_save_dependencies=False)
            attributes.extend(attr)
        elif t.ttype == sql.T.Wildcard:
            # Typicky "SELECT * FROM ..."
            attributes.append(Attribute(name="*"))
        elif t.ttype in sql.T.Literal:
            # Nasli jsme literal (typicky v situaci, kdy je ve WITH definovana pomocna tabulka s konkretnimi -- v SQL kodu zadanymi -- hodnotami)
            attributes.append(Attribute(name=t.value))
        return attributes
    if context == "from" or context == "join":
        # Token je v kontextu FROM ("SELECT ... FROM <token>"), prip. JOIN (napr. "SELECT ... FROM ... INNER JOIN <token>"). V obou pripadech muze byt token jak typu Parenthesis ("SELECT ... FROM ( SELECT ... )", "... JOIN ( SELECT ... )"), tak muze jit o prosty nazev zdrojove tabulky + pripadny alias a komentar.
        # Zde navic mohou nastat dva pripady: bud je za zavorkou alias a/nebo komentar (--> jako statement zpracujeme t.tokens[0]), nebo je v SQL kodu pouze zavorka (--> jako statement zpracujeme cely token).
        if isinstance(t, sql.Parenthesis):
            # Pripadny komentar by byl az za zavorkou, tzn. comment_before muzeme ignorovat
            table = Table(name_template="select", table_type=Table.AUX_TABLE)
            Table.__tables__.append(table)
            process_statement(t, table)
            return [table]
        if isinstance(t.tokens[0], sql.Parenthesis):
            # Struktura t.tokens: parenthesis-SELECT [ whitespace(s) [AS whitespace(s) ] alias [ whitespace(s) komentar ] ]
            # V zavorce je vzdy SELECT, takze je potreba vytvorit odpovidajici (mezi-)tabulku (sablona == "select")
            table = Table(name_template="select", table_type=Table.AUX_TABLE)
            Table.__tables__.append(table)
            # Prvni sub-token (t.tokens[0]) i vsechno ostatni az po pripadny alias ci komentar zatim preskocime.
            i = 1
            while (i < len(t.tokens) and (t.tokens[i].is_whitespace
                    or (t.tokens[i].ttype == sql.T.Keyword and t.tokens[i].normalized == "AS"))):
                i += 1
            # Ted do kolekce components ulozime vse, co je soucasti aliasu
            components = []
            while i < len(t.tokens) and not t.tokens[i].is_whitespace and not is_comment(t.tokens[i]):
                components.append(t.tokens[i].value)
                i += 1
            if len(components) > 0:
                # Alias byl v kodu uveden, takze ho pridame k vytvorene tabulce
                Table.add_alias(alias_table, table.id, "".join(components))
            # Dale zkontrolujeme posledni token, zda je komentarem. Pokud je a u tabulky zaroven neni nastaveny zadny komentar z drivejska (prvni komentar byva obvykle podrobnejsi nez pripadny dalsi k teze tabulce), tento komentar ulozime.
            last_nonws_token = get_last_nonws_token(t.tokens)
            if (is_comment(last_nonws_token)
                    and (table.comment == None or len(table.comment) == 0)):
                table.set_comment(last_nonws_token.value)
            # Uplne nakonec pak zpracujeme prvni subtoken (t.tokens[0]) jako samostatny statement a odkaz na vytvorenou tabulku predame parametrem
            process_statement(t.tokens[0], table)
            return [table]
        if isinstance(t, sql.IdentifierList):
            # Resime situaci "SELECT ... FROM table1 AS alias1, table2 AS alias2, ..."
            tables = []
            for token in t.tokens:
                if not token.is_whitespace and token.ttype != sql.T.Punctuation:
                    # comment_before predavat nemusime, protoze v aktualnim kontextu se nepouziva
                    tables.extend(process_token(token, alias_table=alias_table, context=context))
            return tables
        # V pripade, ze token (prip. prvni subtoken) neni typu Parenthesis, jde o prosty nazev zdrojove tabulky + pripadny alias a komentar. Tyto ziskame jednoduse zavolanim get_name_alias_comment(...). POZOR: potrebujeme vracet list, ve kterem budou informace k tabulce jako tuple (aby bylo vraceni dat funkcni vc. pripadu, kdy je ve FROM vicero zdroju)!
        return [get_name_alias_comment(t)]
    if context == "with":
        # Token je v kontextu WITH -- Identifier ("WITH <token: name AS ( SELECT ... )>"), IdentifierList ("WITH <token: name_1 AS ( SELECT ... ), name_2 AS ( SELECT ... ), ...>"). Muze jit o typ Parenthesis, pokud byl blok ve WITH umele rozdelen na dva tokeny (viz popis BUGu v process_with_element(...))
        if isinstance(t, sql.Identifier) or isinstance(t, sql.Parenthesis):
            # WITH obsahuje pouze jeden blok (docasnou tabulku) --> zpracujeme metodou process_with_element(...); zaroven musime vratit pripadny komentar, ktery je poslednim tokenem v t.tokens, byt se dost mozna vztahuje az k nasledujicimu tokenu
            return process_with_element(t, comment_before)
        if isinstance(t, sql.IdentifierList):
            # Jednotlive tokeny v zpracujeme analogicky pripadu vyse. Pritom je nutne postupne predavat nalezene komentare a nakonec vratit posledni vystup metody process_with_element(...).
            for token in t.tokens:
                comment_before = process_with_element(token, comment_before)
            return comment_before
    if context == "on":

        # TODO: bug s COUNT apod. mozna muze byt relevantni i zde? --> OVERIT

        # Token je v kontextu ON ("SELECT ... JOIN ... ON <token>"). Zde tedy jde o atributy vc. hodnot, ktere u nich pozadujeme
        return get_attribute_conditions(t)
    if context == None and isinstance(t, sql.Parenthesis):
        # Resime samostatny SELECT (typicky na nejvyssi urovni, byt to tak nemusi nutne byt), ktery je obaleny zavorkami. Zde staci znovu zavolat metodu process_statement(...) a zpet do hlavniho kodu vratit None
        process_statement(t, table=alias_table)
        return None
    


def stripTrailingWSCP(sql_components: list) -> None:
    """Odebere ze zadaneho seznamu casti SQL kodu koncove bile znaky (tyto neni duvod ukladat) a uzaviraci zavorku, pokud tato je tesne pred koncovymi bilymi znaky. Metoda upravuje primo zadanou kolekci!"""
    if sql_components == None or len(sql_components) == 0:
        return
    # Pokud jsou na konci bile znaky, musi byt vzdy soucasti posledniho prvku (jakozto soucast sql.Comment.value). Nejprve tedy odstranime koncove bile znaky z sql_components[-1] (plus pripadne i tento prvek, pokud by zmena vyustila v prazdny retezec -- i vicekrat), potom zkontrolujeme/vyresime ")" a nakonec znovu orezeme pripadne koncove bile znaky z nove posledniho prvku kolekce (opet muze vyzadovat vicero odstraneni -- do sql_components standardne ukladame veskere mezery/..., aby zustalo zachovano odsazeni apod.).
    sc = sql_components[-1].rstrip()
    while len(sc) == 0:
        sql_components.pop()
        sc = sql_components[-1].rstrip()
    sql_components[-1] = sc
    # Zavorka je vlivem ukladani token.value vzdy bez uvodnich/koncovych bilych znaku
    if sql_components[-1] == ")":
        sql_components.pop()
    sc = sql_components[-1].rstrip()
    while len(sc) == 0:
        sql_components.pop()
        sc = sql_components[-1].rstrip()
    sql_components[-1] = sc


def process_statement(s, table=None, known_attribute_aliases=False) -> None:
    """Zpracuje cely SQL statement vc. vytvoreni patricnych tabulek"""
    # CTE ... Common Table Expression (WITH, ...)
    # DDL ... Data Definition Language (...)
    # DML ... Data Manipulation Language (SELECT, ...)
    # Tokeny budeme prochazet iteratorem a rovnou budeme preskakovat bile znaky (komentare vsak ne)
    i = 0
    t = s.token_first(skip_ws=True, skip_cm=False)
    # Flag pro predavani informaci o kontextu toho ktereho tokenu (v ruznych kontextech je zpravidla potreba mirne odlisny zpusob zpracovani)
    context = None
    # Predchozi kontext potrebujeme napr. pro pripad "ON ... --> AND (EXISTS-SELECT) AND --> ON"
    prev_context = None
    # Flag pro reseni nestandardnich situaci vlivem chyb v sqlparse (rozdelene tokeny apod.) -- ridi, zda se lze vratit k predchozimu kontextu
    can_switch_to_prev_context = True
    comment_before = ""
    # Pocitadlo radku od posledniho komentare (nekdy nas zajima komentar pred aktualnim tokenem). Pocatecni hodnota je libovolna takova, aby se v cyklu na zacatku NEresetoval comment_before, pokud by SQL dotaz nezacinal komentarem.
    token_counter = 10
    # Zdrojovy kod:
    #   * WITH: lze primo pomoci t.value
    #   * JOIN: nutno skladat po castech (oddelene tokeny)
    #   * SELECT: u "( SELECT ... )" sice lze pouzit t.parent.value, ale toto u top-level SELECT (bez uvedeni v zavorkach) ulozi vzdy kompletne cely (!) SQL dotaz, coz neni zadouci. I zde tedy jsou zdrojove kody skladany po castech.
    sql_components = []
    join_components = []
    join_table = None
    # union_* jsou potreba v pripade, ze sjednocovani je provadeno bez prikazu "SELECT ..." v zavorce (tzn. "SELECT ... UNION SELECT ..."), jelikoz pak je patricny SQL kod vracen jako prosta sekvence tokenu). Pokud je nektery SELECT v zavorkach, zpracovava se jako samostatny statement.
    union_components = []
    union_table = None
    # Pro korektni nastavovani zavislosti budeme potrebovat take odkaz na posledni "SELECT" tabulku (protoze lze narazit napr. na pripad "SELECT ... FROM ... JOIN ... ON ... UNION SELECT ... FROM ... JOIN ... ON ...", kde prvni join_table je zavislosti prvniho selectu, zatimco ve druhem pripade je patricna join_table zavislosti union-selectu).
    if table != None:
        last_select_table_id = table.id
    else:
        last_select_table_id = -1
    # Nekompletni atribut vznikly v dusledku WITHIN GROUP, OVER apod. (viz mj. bugy zminene v process_token(...)); pokud neni None, je potreba ho sloucit s nekompletnim prvnim atributem vracenym v "dalsim kole" zpracovavani atributu
    split_attribute = None
    while t != None:
        if t.ttype == sql.T.Punctuation or t.is_whitespace:
            # Carku apod. pouze ulozime do kolekci sql_components, join_components a union_components (je nutne aktualizovat vsechny!) a nacteme dalsi token
            sql_components.append(t.value)
            join_components.append(t.value)
            union_components.append(t.value)
            (i, t) = s.token_next(i, skip_ws=False, skip_cm=False)
            continue
        # Jsme-li dva tokeny od posleniho komentare, muzeme resetovat comment_before (reset po jednom tokenu nelze, jelikoz jednim z nich muze byt carka mezi SQL bloky a komentar k takovemu bloku pak je typicky na radku pred touto carkou). Je take nutne vzit do uvahy can_switch_to_prev_context -- pokud je False, vime, ze doslo k umelemu rozdeleni tokenu a reset comment_before nelze provest.
        if can_switch_to_prev_context:
            token_counter += 1
            if token_counter == 2:
                comment_before = ""
        # Nektera klicova slova zpusobi vraceni vicero tokenu namisto jednoho -- v takovem pripade se nesmime vratit k predchozimu kontextu driv, nez zpracujeme veskere relevantni tokeny!
        can_switch_to_prev_context = True
        if is_comment(t):
            # Pri nalezeni komentare si tento ulozime jeho druhou cast (za serii pomlcek) a resetujeme token_counter
            comment_before = split_comment(t)[1]
            token_counter = 0
        elif t.ttype == sql.T.Keyword:
            # Narazili jsme na klicove slovo, coz ve vetsine pripadu (viz dale) vyzaduje nastaveni context
            if t.normalized == "FROM":
                prev_context = context
                context = "from"
                # Musime jeste overit, jestli nemame ulozeny nejaky rozdeleny atribut s Literalem (tento mohl byt na konci seznamu bez aliasu, tzn. byl by docasne ve split_attribute a v tabulce by zatim chybel). V takovem pripade nastavime comment = condition = None a atribut pridame do patricne tabulky (table, resp. union_table).
                if split_attribute != None:
                    split_attribute.comment = None
                    split_attribute.condition = None
                    if union_table != None:
                        union_table.attributes.append(split_attribute)
                    table.attributes.append(split_attribute)
                    # Nakonec resetujeme promennou split_attribute
                    split_attribute = None
            elif "JOIN" in t.normalized:
                prev_context = context
                context = "join"
                # Zde musime krome nastaveni kontextu navic resetovat join_components
                join_components = []
            elif t.normalized == "ON":
                if context == "merge":
                    # Jsme-li porad v MERGE ("[MERGE] [INTO] [table AS alias] [USING] [(...) AS alias] [ON] [podminky] [WHEN] [MATCHED] [THEN] [UPDATE] [SET] [...]"), podminky pro upravu dat v DB nas nezajimaji. Nasledujici token tedy preskocime a pak rovnou prejdeme na zacatek cyklu.
                    (i, t) = s.token_next(i, skip_ws=True, skip_cm=False)  # t == [podminky]
                    (i, t) = s.token_next(i, skip_ws=True, skip_cm=False)
                    continue
                else:
                    if join_table == None:
                        raise Exception(f"Klíčové slovo ON nalezeno před klíčovým slovem JOIN")
                    prev_context = context
                    context = "on"
            elif "UNION" in t.normalized:
                prev_context = context
                context = "union-select"
                # Pokud jsme doted resili UNION SELECT (tzn. pokud union_table != None), je nutne ke stavajici union_table pridat zdrojovy SQL kod a resetovat referenci na tabulku (UNION je totiz timto doreseny)
                if union_table != None:
                    # Zdrojovy kod ulozime jedine v pripade, ze zatim nebyl prirazen (muze uz totiz byt ulozen z doby, kdy byl zpracovavan "UNION ( SELECT ... )", tzn. kdy SELECT byl obalen extra zavorkami)
                    if union_table.source_sql == None or len(union_table.source_sql) == 0:
                        union_table.source_sql = "".join(union_components)
                    union_table = None
            elif "EXISTS" in t.normalized:
                # BUG: v SQL kodu je "EXISTS komentar \n ( SELECT ...)" --> opet rozdlene tokeny!
                prev_context = context
                context = "exists-select"
            elif t.normalized == "OVER":
                # Tato cast je nutna pro rucni obejiti chyby v sqlparse (BUG https://github.com/andialbrecht/sqlparse/issues/701 )
                # Klicove slovo OVER a nasledna zavorka s pripadnym PARTITION BY apod. jsou vraceny jako dva tokeny oddelene od predchoziho tokenu s funkci. Pripadny alias a komentar jsou az soucasti tokenu se zavorkou. Prvni token s OVER tedy pridame do sql_components a nasledne z druheho tokenu zjistime pripadny alias a komentar.
                if union_table != None:
                    if len(union_table.attributes) == len(table.attributes):
                        table.attributes.pop()
                    split_attribute = union_table.attributes.pop()
                else:
                    split_attribute = table.attributes.pop()
                # Komentar musime s ohledem na pritomnost mezer priradit primo, nikoliv pomoci set-comment(...)!
                split_attribute.condition = Attribute.CONDITION_SPLIT_ATTRIBUTE
                split_attribute.comment = " OVER "
            elif (t.normalized in ["ORDER BY", "GROUP BY", "CYCLE", "SET", "TO", "DEFAULT", "USING", "WHEN", "THEN"]):
                # V tomto pripade se zda, ze parametry (jeden, prip. vice) jsou vzdy vraceny jako jeden token. Akt. token tedy ulozime do kolekci sql_components, join_components a union_components a nacteme dalsi token (cimz nasledujici token de facto preskocime). V SQL zdroji ale chceme zachovat veskere bile znaky...
                sql_components.append(t.value)
                join_components.append(t.value)
                union_components.append(t.value)
                (i, t) = s.token_next(i, skip_ws=False, skip_cm=False)
                while t.is_whitespace:  # Zde predpokladame, ze t != None (pokud t == None, je s SQL kodem neco spatne a stejne bychom museli parsovani prerusit...)
                    sql_components.append(t.value)
                    join_components.append(t.value)
                    union_components.append(t.value)
                    (i, t) = s.token_next(i, skip_ws=False, skip_cm=False)
                sql_components.append(t.value)
                join_components.append(t.value)
                union_components.append(t.value)
                (i, t) = s.token_next(i, skip_ws=False, skip_cm=False)
                # Zaroven si poznacime, ze se zatim nelze vratit k predchozimu kontextu, protoze jeste mohou nasledovat dalsi relevantni tokeny
                can_switch_to_prev_context = False
            elif t.normalized == "CONNECT":
                # Zde nelze obecne rici, jakym zpusobem budou tokeny vraceny (nepovinna klicova slova, Identifier vs. Builtin + Comparison + Integer vs. ...). Nasledujici tokeny tedy musime prochazet a ukladat do kolekci (sql_components atd.) tak dlouho, nez najdeme Comparison. POZOR: "...ttype != sql.T.Comparison" NENI TOTEZ JAKO "not isinstance(..., sql.Comparison)"!
                while t != None and not (t.ttype == sql.T.Comparison or isinstance(t, sql.Comparison)):
                    sql_components.append(t.value)
                    join_components.append(t.value)
                    union_components.append(t.value)
                    (i, t) = s.token_next(i, skip_ws=False, skip_cm=False)
                # Comparison si ulozime do kolekci
                sql_components.append(t.value)
                join_components.append(t.value)
                union_components.append(t.value)
                # Ted jeste overime, zda je nasl. non-whitespace token Literal, prip. zavorka. Pokud neni, preskocime na zacatek cyklu, jinak pokracujeme na konec cyklu (ulozeni hodnoty + nacteni noveho tokenu).
                (i, t) = s.token_next(i, skip_ws=False, skip_cm=False)
                while t.is_whitespace:  # Zde predpokladame, ze t != None (pokud t == None, je s SQL kodem neco spatne a stejne bychom museli parsovani prerusit...)
                    sql_components.append(t.value)
                    join_components.append(t.value)
                    union_components.append(t.value)
                    (i, t) = s.token_next(i, skip_ws=False, skip_cm=False)
                if not (t.ttype in sql.T.Literal or isinstance(t, sql.Parenthesis)):
                    continue
            elif context == "merge" and t.normalized == "INTO":
                # Akt. token muzeme preskocit (rovnou preskocime i bile znaky), stejne jako ten, ktery po nem nasleduje ("[MERGE] [INTO] [table AS alias] [USING] [(...) AS alias] [ON] [...] [WHEN] [MATCHED] [THEN] [UPDATE] [SET] [...]"; ulozeni stav. tokenu neni potreba)
                (i, t) = s.token_next(i, skip_ws=True, skip_cm=False)  # t == [table AS alias]
                (i, t) = s.token_next(i, skip_ws=True, skip_cm=False)  # t == [USING]
                (i, t) = s.token_next(i, skip_ws=True, skip_cm=False)  # Nyni je v t cast SQL dotazu, ktera nas zajima
                process_statement(t.tokens[0])
                # Nacteme dalsi token, skocime zpet na zacatek cyklu a budeme pokracovat ve zpracovavani MERGE
                (i, t) = s.token_next(i, skip_ws=True, skip_cm=False)
                continue
            else:

                # DEBUG
                if not (t.normalized in ["DISTINCT", "GROUP", "AS"]
                        or (context in ["on", "where"] and t.normalized in ["AND", "OR", "NOT"])):
                    w = f"\n>>> POTENCIALNE PROBLEMATICKE KLICOVE SLOVO: {t.normalized}"
                    print(w)
                    warnings.append(w)

            # Kazde jine vyse neuvedene klicove slovo (u kterych nepredpokladame vyskyt parametru) proste na konci tohoto cyklu ulozime a nacteme dalsi token

        elif t.ttype == sql.T.CTE and t.normalized == "WITH":
            prev_context = context
            context = "with"
        elif t.ttype == sql.T.DML and t.normalized == "SELECT":
            if context == "union-select":
                # Spojovane SELECTy mohou byt vc. WHERE apod. a slouceni vsech atributu takovych SELECTu pod nadrazenou tabulku by nemuselo davat smysl. Pokud tedy po UNION [ALL] nasleduje SELECT (bez uvedeni v zavorkach), budou odpovidajici tokeny vraceny sqlparse postupne a tudiz musime uz zde vytvorit patricnou mezi-tabulku. Jinak receno, k situaci je nutne pristupovat podobně jako u JOIN. Je-li SELECT v zavorkach, zpracuje se dale jako samostatny statement.
                union_table = Table(name_template="union-select", comment=comment_before, table_type=Table.AUX_TABLE)
                Table.__tables__.append(union_table)
                last_select_table_id = union_table.id
                union_components = []
            else:
                prev_context = context
                context = "select"
                # Pokud jde o SELECT na nejvyssi urovni dotazu, neexistuje pro nej zatim zadna tabulka. Tuto tedy vytvorime, aby k ni pak bylo mozne doplnit atributy atd.
                if table == None:
                    table = Table(name_template="main-select", comment=comment_before, table_type=Table.MAIN_SELECT)
                    Table.__tables__.append(table)
                    last_select_table_id = table.id
                    # Tabulka s aliasy (alias_table) zde -- na nejvyssi urovni -- zustava None, takze neni nutne cokoliv nastavovat
                sql_components = []
        elif t.ttype == sql.T.DML and t.normalized == "MERGE":
            prev_context = context
            context = "merge"
        elif isinstance(t, sql.Where):
            # Kontext musime nastavit pro potreby pripadneho pozdejsiho odstraneni fiktivnich atributu se "spojkami" (BUG v sqlparse)
            prev_context = context
            context = "where"
            attributes = get_attribute_conditions(t)
            # Pokud jsme pri nacitani atributu nasli jako posledni sub-token komentar, jde temer jiste o komentar k nasledujicimu bloku SQL kodu. Fiktivni atribut s nesmyslnymi parametry (kontrolovat budeme pro rychlost pouze podle condition == Attribute.CONDITION_COMMENT, comment != None) nyni komentar ziskame zpet a aktualizujeme pomoci nej comment_before.
            if len(attributes) > 0:
                last_attribute = attributes[-1]
                if last_attribute.condition == Attribute.CONDITION_COMMENT:
                    # Komentar ulozime jedine v pripade, ze -- po orezani mezer pod. v konstruktoru -- neni None
                    if last_attribute.comment != None:
                        comment_before = last_attribute.comment
                    else:
                        comment_before = ""
                    attributes.pop()
            # Vznikly pri zpracovavani podminek nejake mezi-tabulky pro "EXISTS ..."? Pokud ano, stavajici tabulku musime nyni navazat na vsechny takove tabulky pomoci vracenych fiktivnich atributu (kontrolovat budeme pro rychlost pouze podle condition == Attribute.CONDITION_EXISTS_SELECT, comment == ID exists_table), ktere jsou pak vzdy jednotlive nasledovany atributem se jmennou referenci (a pripadnym komentarem) k dane mezi-tabulce.
            j = 0
            while j < len(attributes):
                attribute = attributes[j]
                # Nejdrive zkontrolujeme, zda jsme pri parsovani tokenu nenasli placeholder -- pokud ano, je potreba u hlavni tabulky aktualizovat patricny seznam bindovanych promennych
                if attribute.condition == Attribute.CONDITION_PLACEHOLDER_PRESENT:
                    if union_table != None:
                        union_table.add_bind_var(attribute.comment)
                    table.add_bind_var(attribute.comment)
                    attributes.pop(j)
                    continue
                if (attribute.condition == Attribute.CONDITION_DEPENDENCY
                        or attribute.condition == Attribute.CONDITION_EXISTS_SELECT):
                    id = int(attribute.comment)
                    sub_table = Table.get_table_by_id(id)
                    if union_table != None:
                        # Vazba table --> union_table byla nastavena uz drive po nalezeni UNION SELECT
                        union_table.link_to_table_id(id)
                        # Do union_table take rovnou zkopirujeme bindovane promenne
                        sub_table.copy_bind_vars_to_table(union_table)
                    else:
                        table.link_to_table_id(id)
                    # Do hlavni tabulky jeste potrebujeme zkopirovat (a) zjistene aliasy a (b) pripadne placeholdery
                    sub_table.copy_aliases_to_table(table)
                    sub_table.copy_bind_vars_to_table(table)
                    # Nakonec odebereme fiktivni atribut z kolekce obj (index j musi zustat beze zmeny)
                    attributes.pop(j)
                    # V pripade Attribute.CONDITION_EXISTS_SELECT sice ihned nasleduje jeden standardni atribut se jmennou referenci na odpovidajici mezi-tabulku, ktery bychom mohli po nalezite podmince preskocit, ale podobne narocne je proste pouzit zde continue a nasledujici atribut zkontrolovat obvyklym zpusobem.
                    continue
                # Narazit muzeme i na dalsi komentare k nalezenym vnorenym podminkam. Kazdy takovy ulozeny komentar odstranime z kolekce a pokud jemu predchazejici atribut zatim komentar nema, pridame ho. Jinak fiktivni atribut ignorujeme.
                if attribute.condition == Attribute.CONDITION_COMMENT:
                    if j > 0 and (attributes[j - 1].comment == None or len(attributes[j - 1].comment) == 0):
                        attributes[j - 1].set_comment(attributes[j].comment)
                    attributes.pop(j)
                    continue
                # Pokud je pritomen fiktivni atribut se "spojkou" (operatorem), muze -- byt nemusi -- jit o indikator toho, ze okolni standardni (!) atributy byly vlivem chyb v sqlparse umele rozdeleny. Je-li takovy atribut uplne na konci, zatim ho nechame byt; bude doresen, prip. zahozen pri navratu k predchozimu kontextu.
                if (attribute.condition == Attribute.CONDITION_SPLIT_ATTRIBUTE_LINK
                        and j > 0
                        and j < len(attributes) - 1
                        and attributes[j - 1].name != None
                        and attributes[j + 1].name != None):
                    attributes[j - 1].set_name(name + attribute.comment + attributes[j + 1].name)
                    attributes[j - 1].alias = attributes[j + 1].alias
                    if attributes[j - 1].comment == None or len(attributes[j - 1].comment) == 0:
                        attributes[j - 1].comment = attributes[j + 1].comment
                    attributes.pop(j)  # Odebereme fiktivni atribut se "spojkou"...
                    attributes.pop(j)  # ... a zbytek rozdeleneho atributu
                    continue
                j += 1
            # Nyni aktualizujeme podminky v conditions a budouci zavislosti v attributes u patricne tabulky (union_table, resp. table -- dle situace)
            if union_table != None:
                # Aliasy z union_table (byt jde o SELECT) kopirovat nemusime, jelikoz takovy SELECT je zpracovavan bez dalsiho volani process_statement(...), cili pripadne aliasy jsou ukladany primo do table.statement_aliases
                union_table.conditions.extend(attributes)
            else:
                table.conditions.extend(attributes)
        else:
            # Jakykoliv jiny token zpracujeme "obecnou" metodou process_token(...) s tim, ze parametrem predame informaci o kontextu (context) a pripadnem komentari pred tokenem (comment_before). Timto vyresime napr. i tokeny typu "select ... from ... PIVOT (...)" (typ: Function), jelikoz v miste uziti PIVOT uz je context == None, tzn. process_token(...) vrati None.
            obj = process_token(t, table, context, comment_before)
            # Navratova hodnota process_token(...) muze byt ruznych typu v zavislosti na kontextu apod. Na zaklade toho se nyni rozhodneme, jakym konkretnim zpusobem je potreba s ni nalozit.
            if obj != None:
                # Podminka na delku obj resi situaci, kdy v ON apod. nejsou uvedene zadne atributy (mozna jde o chybu, nicmene obcas se vyskytuje v testovacich dotazech...)
                if isinstance(obj, list) and len(obj) > 0:
                    if isinstance(obj[0], Attribute):
                        # Ziskali jsme seznam atributu
                        # BUG: Pokud parser umele rozdeli seznam atributu v SELECT apod. na vice tokenu, muze u prvniho standardniho atributu chybet komentar, ktery byl v SQL kodu uveden nad patricnym radkem. Nejprve tedy projdeme vraceny seznam atributu a kdyz bude u prvniho standardniho atributu komentar prazdny, nahradime ho obsahem promenne comment_before. Pro urychleni ale budeme seznam prochazet jedine v pripade, ze comment_before != "".
                        if len(comment_before) > 0:
                            j = 0
                            while j < len(obj):
                                attribute = obj[j]
                                if attribute.is_standard_attribute():
                                    if attribute.comment == None or len(attribute.name) == 0:
                                        attribute.comment = comment_before
                                    break
                                j += 1
                        # Nyni pokracujeme ve zpracovavani seznamu atributu 
                        if context == "on":
                            # Pokud jsme pri nacitani atributu v "JOIN ... ON ..."" nasli jako posledni sub-token komentar, jde o komentar k mezi-tabulce reprezentujici JOIN. Do seznamu atributu byl v takovem pripade jako posledni pridat fiktivni atribut s nesmyslnymi parametry (kontrolovat budeme pro rychlost pouze podle condition == Attribute.CONDITION_COMMENT, comment != None), ze ktereho nyni komentar ziskame zpet a priradime ho k dane tabulce.
                            if len(obj) > 0:
                                last_attribute = obj[-1]
                                if last_attribute.condition == Attribute.CONDITION_COMMENT:
                                    # Komentar ulozime jedine v pripade, ze -- po orezani mezer pod. v konstruktoru -- neni None
                                    if last_attribute.comment != None:

                                        # TODO: nastavit ke split_attribute, pokud existuje?

                                        join_table.set_comment(last_attribute.comment)
                                    obj.pop()
                                elif last_attribute.condition == Attribute.CONDITION_SINGLE_ELEMENT:
                                    # Ke zpracovani jsme dostali jen jednu cast podminky (LHS, operator, prip. RHS)
                                    if split_attribute == None:
                                        # Zpracovavali jsme LHS (name == jmeno, comment == pripadny komentar)
                                        split_attribute = last_attribute
                                        # Resetujeme podminku, ktera uz nyni neni potreba
                                        split_attribute.condition = None
                                    elif split_attribute.condition == None:
                                        # Zpracovavali jsme operator (name == operator, comment == pripadny komentar)
                                        split_attribute.condition = last_attribute.name
                                        if last_attribute.comment != None and len(last_attribute.comment) > 0:
                                            split_attribute.comment = last_attribute.comment
                                    else:
                                        # Zpracovavali jsme RHS (name == zbytek podminky za operatorem, comment == pripadny komentar)
                                        split_attribute.condition += " " + last_attribute.name
                                        if last_attribute.comment != None and len(last_attribute.comment) > 0:
                                            split_attribute.comment = last_attribute.comment
                                        join_table.conditions.append(split_attribute)
                                        split_attribute = None
                                    obj.pop()
                            # Zkontrolujeme, zda mezi podminkami nebylo "EXISTS ( SELECT ... )", a pripadne aktualizujeme zavislosti a podminky u join_table. Pozor: JOIN je vzdy zavislosti predchoziho selectu (UNION SELECT apod.)!
                            last_select_table = Table.get_table_by_id(last_select_table_id)
                            j = 0
                            while j < len(obj):
                                attribute = obj[j]
                                # Nejprve zkontrolujeme, zda jsme pri parsovani tokenu nenasli placeholder -- pokud ano, je potreba aktualizovat seznam bindovanych promennych jak u join_table (protoze u ni jsme placeholder nasli), tak u nadrazene tabulky (table)
                                if attribute.condition == Attribute.CONDITION_PLACEHOLDER_PRESENT:
                                    join_table.add_bind_var(attribute.comment)
                                    if last_select_table != None:
                                        last_select_table.add_bind_var(attribute.comment)
                                    if last_select_table_id != table.id:
                                        table.add_bind_var(attribute.comment)
                                    obj.pop(j)
                                    continue
                                if (attribute.condition == Attribute.CONDITION_DEPENDENCY
                                        or attribute.condition == Attribute.CONDITION_EXISTS_SELECT):
                                    id = int(attribute.comment)
                                    join_table.link_to_table_id(id)
                                    sub_table = Table.get_table_by_id(id)
                                    # Aliasy nalezene pri zpracovavani subselectu (prip. EXISTS SELECT, UNION SELECT) byly ulozeny do slovniku patricne tabulky a musime je zkopirovat do slovniku aktualniho (nadrazeneho) SELECT
                                    # Jestlize jsme objevili placeholder(y), musime seznam bindovanych promennych predat i do join_table (ktera je nadrazena sub_table) a hlavni tabulky (table, nadrazena join_table). Stejne je potreba upravit union_table, pokud tato aktualne != None.
                                    sub_table.copy_bind_vars_to_table(join_table)
                                    # Kopie udaju do last_select_table lze provest primo, nebot kontrola tab != None je provedena uvnitr kopirovaci metody
                                    sub_table.copy_aliases_to_table(last_select_table)
                                    sub_table.copy_bind_vars_to_table(last_select_table)
                                    # Do table budeme kopirovat jedine v pripade, ze to nebylo provedeno na predchozich dvou radcich
                                    if last_select_table_id != table.id:
                                        sub_table.copy_aliases_to_table(table)
                                        sub_table.copy_bind_vars_to_table(table)
                                    obj.pop(j)
                                    # V pripade Attribute.CONDITION_EXISTS_SELECT sice ihned nasleduje jeden standardni atribut se jmennou referenci na odpovidajici mezi-tabulku, ktery bychom mohli po nalezite podmince preskocit, ale podobne narocne je proste pouzit zde continue a nasledujici atribut zkontrolovat obvyklym zpusobem.
                                    continue
                                # Narazit muzeme i na dalsi komentare k nalezenym vnorenym podminkam. Kazdy takovy ulozeny komentar odstranime z kolekce a pokud jemu predchazejici atribut zatim komentar nema, pridame ho. Jinak fiktivni atribut ignorujeme.
                                if attribute.condition == Attribute.CONDITION_COMMENT:
                                    if j > 0 and (obj[j - 1].comment == None or len(obj[j - 1].comment) == 0):
                                        obj[j - 1].set_comment(attribute.comment)
                                    obj.pop(j)
                                    continue
                                # Pokud je pritomen fiktivni atribut se "spojkou" (operatorem), muze -- byt nemusi -- jit o indikator toho, ze okolni standardni (!) atributy byly vlivem chyb v sqlparse umele rozdeleny. Je-li takovy atribut uplne na konci, zatim ho nechame byt; bude doresen, prip. zahozen pri navratu k predchozimu kontextu.
                                if (attribute.condition == Attribute.CONDITION_SPLIT_ATTRIBUTE_LINK
                                        and j > 0
                                        and j < len(obj) - 1
                                        and obj[j - 1].name != None
                                        and obj[j + 1].name != None):
                                    obj[j - 1].set_name(name + attribute.comment + obj[j + 1].name)
                                    obj[j - 1].alias = obj[j + 1].alias
                                    if obj[j - 1].comment == None or len(obj[j - 1].comment) == 0:
                                        obj[j - 1].comment = obj[j + 1].comment
                                    obj.pop(j)  # Odebereme fiktivni atribut se "spojkou"...
                                    obj.pop(j)  # ... a zbytek rozdeleneho atributu
                                    continue
                                j += 1
                            # Vraceny objekt (nyni uz bez pripadneho fiktivniho atributu s komentarem) muzeme pouzit k aktualizaci atributu i mezitabulky reprezentujici JOIN. Budouci zavislosti z kolekce future_dependencies pridame do table.attributes.
                            join_table.conditions.extend(obj)
                        elif "select" in context:
                            # Projdeme vraceny seznam, ktery muze obsahovat fiktivni atributy s ID tabulek (kontrolovat budeme pro rychlost pouze podle condition == Attribute.CONDITION_DEPENDENCY; comment == ID tabulky), na nichz zavisi aktualne resena tabulka (typicky scenar: namisto obycejneho atributu je v SELECT uveden dalsi SELECT)
                            j = 0
                            while j < len(obj):
                                attribute = obj[j]
                                if attribute.condition == Attribute.CONDITION_DEPENDENCY:
                                    id = int(attribute.comment)
                                    subselect_table = Table.get_table_by_id(id)
                                    if union_table != None:
                                        union_table.link_to_table_id(id)
                                        # Krome svazani tabulek jeste potrebujeme zkopirovat do union_table (a) zjistene aliasy a (b) pripadne placeholdery
                                        subselect_table.copy_aliases_to_table(union_table)
                                        subselect_table.copy_bind_vars_to_table(union_table)
                                    else:
                                        table.link_to_table_id(id)
                                    # Zjistene aliasy a pripadne placeholdery je potreba zkopirovat i do hlavni tabulky
                                    subselect_table.copy_aliases_to_table(table)
                                    subselect_table.copy_bind_vars_to_table(table)
                                    # Nakonec odebereme fiktivni atribut z kolekce obj (index j musi zustat beze zmeny)
                                    obj.pop(j)
                                    continue
                                if attribute.condition == Attribute.CONDITION_PLACEHOLDER_PRESENT:
                                    if union_table != None:
                                        union_table.add_bind_var(attribute.comment)
                                    table.add_bind_var(attribute.comment)
                                    # Fiktivni atribut musime odebrat z kolekce obj (index j zustava beze zmeny)
                                    obj.pop(j)
                                    continue
                                # Pokud je pritomen fiktivni atribut se "spojkou" (operatorem), muze -- byt nemusi -- jit o indikator toho, ze okolni standardni (!) atributy byly vlivem chyb v sqlparse umele rozdeleny. Je-li takovy atribut uplne na konci, zatim ho nechame byt; bude doresen, prip. zahozen pri navratu k predchozimu kontextu.
                                if (attribute.condition == Attribute.CONDITION_SPLIT_ATTRIBUTE_LINK
                                        and j > 0
                                        and j < len(obj) - 1
                                        and obj[j - 1].name != None
                                        and obj[j + 1].name != None):
                                    obj[j - 1].set_name(name + attribute.comment + obj[j + 1].name)
                                    obj[j - 1].alias = obj[j + 1].alias
                                    if obj[j - 1].comment == None or len(obj[j - 1].comment) == 0:
                                        obj[j - 1].comment = obj[j + 1].comment
                                    obj.pop(j)  # Odebereme fiktivni atribut se "spojkou"...
                                    obj.pop(j)  # ... a zbytek rozdeleneho atributu
                                    continue
                                j += 1
                            # Dale musime zkontrolovat, jestli nemame ze zpracovavani minuleho tokenu nekomplentni atribut (BUG: WITHIN GROUP apod.). Pokud ne, zkontrolujeme posledni nyni vraceny atribut, zda nahodou neni takovym objektem. Jestlize naopak nekompletni atribut mame, sloucime ho s prvnim nyni vracenym atributem (ktery nasledne odebereme z obj) a takto vznikly kompletni atribut pridame k tabulce. Zde nelze rovnou resetovat split_attribute, jelikoz i zde muze byt posledni atribut opet nekompletni...
                            # Ve zbylem kodu pro zpracovani vracenych atributu (vc. pripadneho nekompletniho z minula) uz budeme pracovat jen s aktualne resenou tabulkou (at uz pujde o table, nebo union_table). Pro zjednoduseni kodu tedy pouzijeme novou promennou obsahujici referenci na patricnou tabulku.
                            if split_attribute == None:
                                # "Rozdeleny" atribut je v kolekci obj vzdy jako posledni --> index == -1
                                if (obj[-1].condition == Attribute.CONDITION_SPLIT_ATTRIBUTE
                                        or obj[-1].condition == Attribute.CONDITION_SPLIT_ATTRIBUTE_ALIAS_MISSING):
                                    split_attribute = obj.pop()
                            else:
                                # Zbytek rozdeleneho atributu je hned na zacatku kolekce
                                attr_remainder = obj.pop(0)
                                # Pokud napr. mezi "GROUP" a nasledujici zavorkou neni mezera, je pokracovani tokenu vc. klicoveho slova "GROUP". Zkontrolujeme tedy, zda jmeno attr_remainder zacina na "(" -- pokud ne a spojovaci reteze (split_attribute.comment) zaroven obsahuje vice nez jedno slovo, to posledni z nej odstranime.
                                if split_attribute.condition == Attribute.CONDITION_SPLIT_ATTRIBUTE:
                                    attr_link = split_attribute.comment.split()
                                    if not attr_remainder.name.startswith("(") and len(attr_link) > 1:
                                        attr_link.pop()
                                        # Komentar musime nastavit primo (mezery!), nikoliv pomoci set-comment(...)
                                        split_attribute.comment = " " + " ".join(attr_link) + " "
                                    # Standardni rozdeleny atribut
                                    split_attribute.set_name(f"{split_attribute.name}{split_attribute.comment}{attr_remainder.name}")
                                    split_attribute.alias = attr_remainder.alias
                                else:
                                    # Rozdeleny atribut s Literalem
                                    split_attribute.alias = attr_remainder.name
                                # condition musime resetovat na None, jinak by zustavalo se spec. hodnotou napr. pri "SELECT x, y result FROM ..."
                                split_attribute.condition = None
                                # Tady by nejspis take slo vzit komentar tak, jak je, ale pro poradek vyuzijeme set_comment(...)
                                split_attribute.set_comment(attr_remainder.comment)
                                if union_table != None:
                                    union_table.attributes.append(split_attribute)
                                table.attributes.append(split_attribute)
                                if (len(obj) > 0
                                        and (obj[-1].condition == Attribute.CONDITION_SPLIT_ATTRIBUTE
                                        or obj[-1].condition == Attribute.CONDITION_SPLIT_ATTRIBUTE_ALIAS_MISSING)):
                                    split_attribute = obj.pop()
                                else:
                                    split_attribute = None
                            # Nakonec k tabulce pridame atributy zbyle v obj (musime ale zohlednit pripadnou znalost aliasu!)
                            if known_attribute_aliases:
                                # Zde resime blok ve WITH, u ktereho byly za nazvem docasne tabulky uvedeny aliasy (alespon nekterych) atributu. Predchystane ("TBD") atributy ale nelze primo aktualizovat, protoze v dusledku chyb v sqlparse mohlo dojit k umelemu rozdleni tokenu, tzn. zatim nemusime mit k dispozici kompletni sadu atributu. Aktualizujeme proto prvnich len(obj) "TBD" atributu v cilove tabulce s tim, ze kontrolu zbylych "TBD" atributu (vc. pripadneho vyvolani vyjimky) provedeme az uplne na konci process_statement(...).
                                # Vime, ze aliasy atributu tabulky ve WITH musely byt uvedeny ve stejnem poradi jako atributy nyni zjistene z prikazu SELECT. Atributy u tabulky proto na zaklade jejich poradi aktualizujeme podle objektu vraceneho vyse metodou process_token(...).
                                # ALE: v obj se mohou vyskytovat fiktivni atributy indikujici pouziti placeholderu (condition == Attribute.CONDITION_PLACEHOLDER_PRESENT), ktere zde musime preskocit.
                                # Aktualizaci provedeme nejprve u table (pomoci obj). Nasledne, pokud resime UNION SELECT, "zrcadlime" atributy z table do union_table (je to tak jedodussi a asi i rychlejsi nez srovnavat hodnoty atd.).
                                j = 0
                                k = 0
                                while (j < len(table.attributes)):
                                    # Nejprve najdeme nasledujici "TBD" atribut
                                    while (j < len(table.attributes)
                                            and table.attributes[j].condition != Attribute.CONDITION_TBD):
                                        j += 1
                                    if j == len(table.attributes):
                                        # Dosli jsme na konec table.attributes, tzn. uz tam neni zadny dalsi "TBD" atribut
                                        break
                                    # Ted v obj preskocime vsechny pripadne fiktivni atributy s informacemi o placeholderech (staci kontrolovat pomoci name == None)
                                    while k < len(obj) and obj[k].name == None:
                                        k += 1
                                    if k == len(obj):
                                        # Dosli jsme na konec obj, tzn. uz tam neni zadny dalsi standardni atribut, pomoci ktereho bychom mohli aktualizovat pripadne zbyle "TBD" atributy v cilove tabulce
                                        break
                                    attr = obj.pop(k)
                                    # Pokud jsme nasli atribut "*" (prip. "table.*"), zahodime ho, zvysime index j (pro posun na dalsi atribut v table.attributes) a pokracujeme zpet na zacatek cyklu
                                    if attr.name.endswith("*"):
                                        j += 1
                                        continue
                                    table.attributes[j].set_name(attr.name)
                                    # Neni nahodou drive zjisteny alias identicky s tim, co bylo v SELECT? Pokud ano, alias odstranime. Alias zde nemuze byt None (drive byl atribut ve tvaru name == condition == Attribute.CONDITION_TBD + s nastavenym aliasem), takze jmeno a alias muzeme porovnavat bez jakekoliv dalsi kontroly.
                                    if attr.name == table.attributes[j].alias:
                                        table.attributes[j].alias = None
                                    table.attributes[j].condition = attr.condition
                                    # Komentar muzeme aktualizovat primo (bez vyuziti set_comment(...))
                                    table.attributes[j].comment = attr.comment
                                    j += 1
                            # Nakonec pridame pripadne dalsi atributy, ktere byly zjisteny nad ramec aliasu uvedenych za nazvem tabulky -- opet do table i union_table (pokud zrovna resime UNION SELECT). Musime ale zohlednit pripad, kdy je kompletni vycet atributu v puvodnim SELECT i nyni v UNION SELECT, jinak by u tabulky byly atributy uvedeny dvakrat.
                            if union_table != None:
                                union_table.add_missing_attributes(obj)
                                table.add_missing_attributes(obj)
                            else:
                                # Zde pracujeme pouze se standardni tabulkou a muzeme tedy atributy pridat primo pomoci .extend(...)
                                table.attributes.extend(obj)
                        else:
                            # Ve zbylych situacich staci pridat nalezene atributy k aktualni tabulce (ktera uz u korektniho SQL kodu nyni nemuze byt None)
                            table.attributes.extend(obj)
                    else:
                        # Ve vracenem seznamu jsou informace o zdrojovych tabulkach (z "FROM ..."), cili pro kazdou z nich provedeme potrebne upravy. POZOR: prvky kolekce mohou byt typu tuple nebo Table podle toho, co jsme nasli v SQL kodu!
                        for src_table_info in obj:
                            if isinstance(src_table_info, tuple) and isinstance(src_table_info[0], str):
                                # Metoda process_token(...) vratila ntici, v niz je prvni prvek retezcem. Jinak receno, ziskali jsme nazev tabulky spolu s pripadnym aliasem a komentarem. Nejprve tedy zkusime najit zdrojovou tabulku, odkud se berou data, a pridame k ni alias.
                                src_table = Table.get_table_by_name(name=src_table_info[0], alias_table=table)
                                if src_table == None:
                                    # Zdrojova tabulka zatim neexistuje (typicky v situaci, kdy resime "SELECT ... FROM dosud_nezminena_tabulka") --> vytvorime ji
                                    src_table = Table(name=src_table_info[0], comment=src_table_info[2])
                                    Table.__tables__.append(src_table)
                                else:
                                    # Komentar pridame jen v pripade, ze tento zatim neni nastaveny (prvotni komentar zpravidla byva detailnejsi a nedava smysl ho prepsat necim dost mozna kratsim/strucnejsim)
                                    if src_table.comment == None or len(src_table.comment) == 0:
                                        # Komentar by asi slo vzit primo, ale pro poradek vyuzijeme set_comment(...)
                                        src_table.set_comment(src_table_info[2])
                                Table.add_alias(table, src_table.id, src_table_info[1])
                                # V tuto chvili by uz last_select_table_id melo byt za vsech okolnosti >= 0, tzn. last_select_table != None
                                last_select_table = Table.get_table_by_id(last_select_table_id)
                                if context == "join":
                                    # Pokud resime JOIN, vytvorime patricnou mezi-tabulku (zatim neexistuje!), ke ktere budou nasledne pridany atributy s podminkami dle ON
                                    join_table = Table(name_template="join", table_type=Table.AUX_TABLE)
                                    Table.__tables__.append(join_table)
                                    # Navic je nutne nastavit zavislosti tabulek: table (prip. union_table) --> join_table --> src_table
                                    join_table.link_to_table_id(src_table.id)
                                    last_select_table.link_to_table_id(join_table.id)
                                else:
                                    # V "obecnem" pripade ("SELECT ... FROM src_table", UNION SELECT) proste jen k aktualni tabulce reprezentujici SELECT pridame zavislost na zdrojove tabulce. Tabulku s aliasy (alias_table) uz netreba nastavovat, jelikoz toto bylo provedeno drive.
                                    last_select_table.link_to_table_id(src_table.id)
                                    if last_select_table_id != table.id:
                                        table.link_to_table_id(last_select_table_id)
                            elif isinstance(src_table_info, Table):
                                # Metoda process_token(...) vratila objekt typu Table. Toto muze nastat ve dvou pripadech: bud resime JOIN (k cemuz musime vytvorit mezi-tabulku a nastavit odpovidajici zavislosti), nebo jde o situaci "SELECT ... FROM ( SELECT ... )" (kde uz mezi-tabulka byla vytvorena -- jde o tu vracenou -- a pouze nastavime zavislost aktualni tabulky na mezi-tabulce). Aktualne by last_select_table-id melo vzdy byt >= 0.
                                last_select_table = Table.get_table_by_id(last_select_table_id)
                                if context == "join":
                                    join_table = Table(name_template="join", table_type=Table.AUX_TABLE)
                                    Table.__tables__.append(join_table)
                                    # Zavislosti: last_select_table --> join_table --> src_table
                                    last_select_table.link_to_table_id(join_table.id)
                                    join_table.link_to_table_id(src_table_info.id)
                                    # Pripadne placeholdery musime zkopirovat i do join_table (kopirovani do table probehne nize)
                                elif prev_context == "on":  # Zde musime kontrolovat prev_context, jelikoz pro "ON EXISTS (SELECT ... )" je aktualni kontext jiny!
                                    join_table.link_to_table_id(src_table_info.id)
                                    src_table_info.copy_bind_vars_to_table(join_table)
                                    join_table.conditions.append(Attribute(name=get_op_prefix([f"<{src_table_info.name}>"]), comment=comment_before))
                                else:
                                    last_select_table.link_to_table_id(src_table_info.id)
                                # Do hlavni tabulky jeste potrebujeme zkopirovat (a) zjistene aliasy a (b) pripadne placeholdery
                                src_table_info.copy_aliases_to_table(last_select_table)
                                src_table_info.copy_bind_vars_to_table(last_select_table)
                                if last_select_table_id != table.id:
                                    src_table_info.copy_aliases_to_table(table)
                                    src_table_info.copy_bind_vars_to_table(table)
                                # Byla zavorka za UNION? Pokud ano, musime upravit last_select_table_id 
                                # Pokud by union_table byla nactena kompletne, bylo by nyni vse hotovo. Vlivem chyb v sqlparse ale muze byt kod rozdelen na vice tokenu, takze nyni je potreba jeste aktualizovat last_select_table_id a union_table.
                                if src_table_info.name.startswith("union-select-"):
                                    last_select_table_id = src_table_info.id
                                    union_table = src_table_info
                elif context == "with":
                    # Resime blok WITH, kde navratovou hodnotou je pripadny komentar (byva vracen vzdy jako posledni sub-token, i kdyz se muze tykat az nasledujiciho tokenu)
                    # K obejiti BUGu v sqlparse muze nekdy byt vracen tuple namisto retezce -- v takovem pripade namisto resetu token_counter snizime hodnotu promenne o 1 a zakazeme reset kontextu
                    comment_before = obj
                    if isinstance(obj, tuple):
                        # Token byl sqlparse umele rozdelen na dva...
                        token_counter -= 1
                        can_switch_to_prev_context = False
                    else:
                        token_counter = 0
            # Pokud neexistuje zadny nekompletni atribut (dalsi BUG: WITHIN GROUP, OVER, ...), resp. nenasleduje problematicke klicove slovo, ktere by zpusobilo vraceni vicero tokenu namisto jednoho, se muzeme vratit k predchozimu kontextu. K tomu ale musime taktez zkontrolovat nasledujici token. Podobne overime, jestli nenasleduje AND, coz by znacilo napr. pokracovani podminky v JOIN ... ON podm1 AND podm2 AND ... V SQL kodu chceme zachovat veskere bile znaky!
            token_was_operator = (t.ttype in sql.T.Operator)
            (j, next_token) = s.token_next(i, skip_ws=False, skip_cm=False)
            while next_token != None and next_token.is_whitespace:
                sql_components.append(t.value)
                join_components.append(t.value)
                union_components.append(t.value)
                i = j
                t = next_token
                (j, next_token) = s.token_next(i, skip_ws=False, skip_cm=False)
            if next_token != None:
                # Hinty MATERIALIZE a NO_STAR_TRANSFORMATION jsou vraceny jako Identifier, "USE_HASH (arg)" a "USE_NL (arg)" potom jako Function (tzn. zaroven se zavorkou s argumenty). Mezera mezi hintem a zavorkou bude pritomna vzdy, jelikoz toto mame predem osetreno kosmetickou nahradou vq zdrojovem SQL.
                next_token_upper = next_token.value.upper()
                while (next_token_upper in ["MATERIALIZE", "NO_STAR_TRANSFORMATION"]
                        or next_token_upper.startswith("USE_HASH (")
                        or next_token_upper.startswith("USE_NL (")):
                    # Ulozime hodnotu akt. tokenu
                    sql_components.append(t.value)
                    join_components.append(t.value)
                    union_components.append(t.value)
                    # Aktualizujeme index a akt. token
                    i = j
                    t = next_token
                    # Je v tokenu jako posledni non-whitespace subtoken ulozen komentar? (muze se stat v pripade hintu vracenych jako funkce) -- musime obalit try-except, jelikoz .tokens nemusi existovat!
                    try:
                        last_nonws_subtoken = get_last_nonws_token(next_token.tokens)
                        if is_comment(last_nonws_subtoken):
                            # Chcceme druhou cast komentare (pokud existuji obe), protoze ta prvni se pravdepodobne tyka samotneho hintu
                            comment_before = split_comment(last_nonws_subtoken)[1]
                    except:
                        pass
                    # Nacteme novy next_token (chceme zachovat veskere bile znaky!)
                    (j, next_token) = s.token_next(i, skip_ws=False, skip_cm=False)
                    while next_token != None and next_token.is_whitespace:
                        sql_components.append(t.value)
                        join_components.append(t.value)
                        union_components.append(t.value)
                        i = j
                        t = next_token
                        (j, next_token) = s.token_next(i, skip_ws=False, skip_cm=False)
                    if next_token != None:
                        next_token_upper = next_token.value.upper()
                    else:
                        next_token_upper = None
                    # Musime take zakazat reset kontextu
                    can_switch_to_prev_context = False
                # Zde musime znovu zkontrolovat, zda i pripadny novy next_token neni None atd.
                if next_token != None:
                    next_token_upper = next_token.value.upper()
                    if next_token_upper == "WITHIN":
                        # V predchozim tokenu jsme zpracovavali agregacni funkci, za niz byl hned uveden komentar, a "WITHIN" tedy nebylo soucasti daneho tokenu. Slo vsak o rozdeleny atribut (coz jsme tehy jeste nevedeli), takze ted najdeme prvni standardni atribut (asi staci pomoci name != None) od konce table.attributes a presuneme ho do split_attribute (vc. nastaveni condition). (Takovy atribut urcite v table.attributes existuje.) "WITHIN" je v tomto pripade vraceno jako Identifier, ale to zde neni podstatne. Dulezite je, ze se zatim nelze vratit k predchozimu kontextu, coz zaridime uz tim, ze bude split_attribute != None.
                        if union_table != None:
                            j = len(union_table.attributes) - 1
                            while j >= 0:
                                if union_table.attributes[j].name != None:
                                    if len(union_table.attributes) == len(table.attributes):
                                        table.attributes.pop()
                                    split_attribute = union_table.attributes.pop()
                                    break
                                j -= 1
                        else:
                            j = len(table.attributes) - 1
                            while j >= 0:
                                if table.attributes[j].name != None:
                                    split_attribute = table.attributes.pop()
                                    break
                                j -= 1
                        split_attribute.condition == Attribute.CONDITION_SPLIT_ATTRIBUTE
                        split_attribute.comment = " WITHIN GROUP "
                        # Pokracovat budeme standardne na konci cyklu ulozenim tokenu do kolekci atd. Nasledujici token by pak mel byt "GROUP", prip. "GROUP(...)", ktery zpracujeme obvyklym zpusobem.
                    else:
                        # Zde kontrolujeme radeji uppercase verzi, jelikoz ne vse ze seznamu nize je oznaceno za klicove slovo, kde .normalized vraci velka pismena. Zaroven se divame, jestli dalsi token neni carka, protoze pokud by byl, znamenalo by to, ze vycet Identifieru apod. byl v dusledku BUGu umele rozdelen na vicero tokenu (tedy bude jeste mit pokracovani).
                        # Pokud jsem nasli operator, mohl byl token vlivem BUGu v sqlparse umele rozdelen vlozenim "komentar \n" do operace apod. Reset kontextu tedy zatim pro jistotu zakazeme.
                        can_switch_to_prev_context = (can_switch_to_prev_context
                                and (context == "exists-select"  # Po zpracovani EXIST SELECT se potrebujeme vratit k predchozimu kontextu
                                or not (token_was_operator
                                or next_token.ttype in sql.T.Operator
                                or next_token_upper in ["OVER", "AND", "OR", "NOT", "CYCLE", "SET", "TO", "DEFAULT", "USING", "DISTINCT", ","])))
            if can_switch_to_prev_context and split_attribute == None:
                # Pokud resime JOIN a dokoncili jsme parsovani casti ON, muzeme k tabulce ulozit jeji SQL kod
                if context == "on" and join_table != None:
                    # Hodnotu tokenu si pridame to kolekce s komponentami zdrojoveho SQL kodu
                    join_components.append(t.value)
                    # Jelikoz nyni mame cely JOIN zpracovany, lze k mezi-tabulce priradit i ji odpovidajici SQL kod. Referenci na tabulku ale resetovat nesmime! (na rozdil od union_table, kde je toto potreba). Na rozdil od [UNION] SELECT take nemusime z kolekce join_components odebirat koncove bile znaky/uzaviraci zavorku, protoze tyto se v kolekci nenachazi.
                    join_table.source_sql = "".join(join_components)
                context = prev_context
        # Nakonec si ulozime kod tokenu do kolekci sql_components, join_components a union_components (je nutne aktualizovat vsechny!) a nacteme dalsi token
        sql_components.append(t.value)
        join_components.append(t.value)
        union_components.append(t.value)
        (i, t) = s.token_next(i, skip_ws=False, skip_cm=False)
    # Jestlize byl UNION SELECT na konci statementu, chybi u nej zatim SQL kod. Tento tedy nyni pridame.
    if union_table != None:
        # Doresime pripadne zbyle fiktivni atributy se "spojkami", pokud se ukazalo, ze rozdeleni na vice tokenu nebylo uprosted nektereho z prvku vyctu v SELECT/ON/WHERE
        union_table.attributes = process_remaining_link_attributes(union_table.attributes)
        union_table.conditions = process_remaining_link_attributes(union_table.conditions, copy_conditions=True)
        stripTrailingWSCP(union_components)
        # Zdrojovy kod ulozime jedine v pripade, ze zatim nebyl prirazen (muze uz totiz byt ulozen z doby, kdy byl zpracovavan "UNION ( SELECT ... )", tzn. kdy SELECT byl obalen extra zavorkami)
        if union_table.source_sql == None or len(union_table.source_sql) == 0:
            union_table.source_sql = "".join(union_components)
    # Nyni zkontrolujeme, zda v kolekci atributu nezustal nejaky "TBD" (drive zkontrolovat neslo, protoze tokeny jsou nekdy v dusledku chyb v sqlparse umele rozdelene). Pro podchyceni (primarne asi testovacich?) pripadu s blokem/y ve WITH, ale bez alespon jednoho hlavniho SELECT, musime kontrolovat, zda table neni None.
    if table != None:
        for attribute in table.attributes:
            if attribute.condition == Attribute.CONDITION_TBD:
                raise Exception(f"Počet aliasů atributů uvedených explicitně u tabulky {table.name} je větší než počet atributů vrácených v části SELECT")
        # Doresime pripadne zbyle fiktivni atributy se "spojkami", pokud se ukazalo, ze rozdeleni na vice tokenu nebylo uprosted nektereho z prvku vyctu v SELECT/ON/WHERE
        table.attributes = process_remaining_link_attributes(table.attributes)
        table.conditions = process_remaining_link_attributes(table.conditions, copy_conditions=True)
        # Obsah sql_components se resetuje pri nalezeni SELECT, resp. JOIN. Pokud je SELECT v zavorkach ("SELECT ... FROM ( SELECT ... )"), obsahuje kolekce na konci jednu uzaviraci zavorku navic, kterou je pred ulozenim SQL kodu nutne odstranit.
        if len(sql_components) > 0 and sql_components[0].lower() == "select":
            stripTrailingWSCP(sql_components)
            table.source_sql = "".join(sql_components)


# TODO: kontrolovat pritomnost lib. fiktivniho atributu + pripadne Exception, pokud u tabulky neco zbylo?


def process_remaining_link_attributes(attributes: list, copy_conditions=False) -> list:
    """Projde zadany seznam atributu (prip. podminek) a zpracuje pripadne zbyle fiktivni atributy s condition == Attribute.CONDITION_SPLIT_ATTRIBUTE_LINK. Metoda vraci seznam atributu ve stavu po provedeni pripadnych zmen."""
    j = 1
    while j < len(attributes) - 1:
        if (attributes[j].condition == Attribute.CONDITION_SPLIT_ATTRIBUTE_LINK
            and attributes[j - 1].name != None
            and attributes[j + 1].name != None):
            if attributes[j].comment.startswith(" ") and attributes[j].comment.endswith(" "):
                link = attributes[j].comment
            else:
                link = " " + attributes[j].comment + " "
            attributes[j - 1].set_name(attributes[j - 1].name + link + attributes[j + 1].name)
            attributes[j - 1].alias = attributes[j + 1].alias
            if copy_conditions:
                attributes[j - 1].condition = attributes[j + 1].condition
            if attributes[j - 1].comment == None or len(attributes[j - 1].comment) == 0:
                attributes[j - 1].comment = attributes[j + 1].comment
            attributes.pop(j)  # Odebereme fiktivni atribut se "spojkou"...
            attributes.pop(j)  # ... a zbytek rozdeleneho atributu
            continue
        j += 1
    # Pripadny fiktivni atribut se "spojkou" uplne na zacatku nebo na konci kolekce zahodime (nemohl byt dusledkem rozdeleni atributu na vicero casti, protoze za nim uz nic nenasleduje)
    if len(attributes) > 0 and attributes[0].condition == Attribute.CONDITION_SPLIT_ATTRIBUTE_LINK:
        attributes.pop(0)
    if len(attributes) > 0 and attributes[-1].condition == Attribute.CONDITION_SPLIT_ATTRIBUTE_LINK:
        attributes.pop()
    return attributes


def text_to_dia(text: str) -> str:
    """Vrati text ve tvaru vhodnem pro vlozeni do .dia"""
    if text != None and len(text) > 0:
        # Pocatecni a koncove bile znaky orezeme
        return text.replace("<", "&lt;").replace(">", "&gt;").replace("&", "&amp;")
    return ""


def generateDiaBlockAttrCode(name, comment) -> str:
    """Vrati XML kod atributu v Dia bloku "UML Class", kde name je jmeno atributu a comment u nej vlozeny komentar"""
    code = []
    code.append(("        <dia:composite type=\"umlattribute\">\n"
                 "          <dia:attribute name=\"name\">\n"))
    code.append(f"            <dia:string>#{text_to_dia(name)}#</dia:string>\n")
    code.append(("          </dia:attribute>\n"
                 "          <dia:attribute name=\"type\">\n"
                 "            <dia:string>##</dia:string>\n"
                 "          </dia:attribute>\n"
                 "          <dia:attribute name=\"value\">\n"
                 "            <dia:string>##</dia:string>\n"
                 "          </dia:attribute>\n"))
    code.append( "          <dia:attribute name=\"comment\">\n")
    code.append(f"            <dia:string>#{text_to_dia(comment)}#</dia:string>\n")
    code.append(("          </dia:attribute>\n"
                 "          <dia:attribute name=\"visibility\">\n"
                 "            <dia:enum val=\"3\"/>\n"
                 "          </dia:attribute>\n"
                 "          <dia:attribute name=\"abstract\">\n"
                 "            <dia:boolean val=\"false\"/>\n"
                 "          </dia:attribute>\n"
                 "          <dia:attribute name=\"class_scope\">\n"
                 "            <dia:boolean val=\"false\"/>\n"
                 "          </dia:attribute>\n"
                 "        </dia:composite>\n"))
    return "".join(code)


def get_primary_linked_ids(table: Table, path=[]) -> list:
    """Vrati ID vsech tabulek z WITH bloku (table_type == Table.WITH_TABLE), resp. SELECT na nejvyssi urovni (table_type == Table.MAIN_SELECT), ktere jsou bud primo uvedeny v linked_to_tables_id, prip. jsou dosazitelne skrze nepreruseny retezec mezi-tabulek (table_type == Table.AUX_TABLE) zacinajici nekterym z ID v linked_to_tables_id. Parametr path zabranuje zacykleni (pokud jsme uz jednou pres aktualni tabulku prosli, hledani ukoncime)."""
    if table == None or len(table.linked_to_tables_id) == 0 or table.id in path:
        return []
    path.append(table.id)
    primary_linked_ids = []
    for id in table.linked_to_tables_id:
        t = Table.get_table_by_id(id)
        if t.table_type == Table.WITH_TABLE or t.table_type == Table.MAIN_SELECT:
            # Je tabulka s danym ID primo WITH tabulkou, prip. SELECT na nejvyssi urovni? Pokud ano, pridame toto ID do linked_ids a pokracujeme kontrolou dalsiho ID z linked_to_tables_id.
            primary_linked_ids.append(t.id)
        elif t.table_type == Table.AUX_TABLE:
            # Pokud prave resime mezi-tabulku, musime rekurzivne zkontrolovat veskera ID, se kterymi je tato tabulka svazana
            primary_linked_ids.extend(get_primary_linked_ids(t, path))
        # Standardni tabulky (ty z databaze) kontrolovat nemusime, protoze pres ne urcite nemuze vest retezec zavislosti WITH tabulek
    return primary_linked_ids


def get_random_string(n: int) -> str:
    """Vraci nahodny retezec o zadane delce (n znaku) slozeny pouze z malych pismen"""
    return "".join(secrets.choice(string.ascii_lowercase) for _ in range(n))


def replace_match_case(old_str, new_str, text, whole_words=True):
    """Nahradi vsechny vyskyty retezce old_str (musi jit o cela slova) v retezci text retezcem new_str. V pocatecnich znacich new_str az do delky min(len(old_str), len(new_str)) je zachovana velikost pismen, pripadna dalsi pismena az do konce new_str jsou ponechana tak, jak byla zadana."""

    def f_match_case(match):
        g = match.group()
        res = []
        min_len = min(len(g), len(new_str))
        for i in range(min_len):
            if g[i].isupper():
                res.append(new_str[i].upper())
            else:
                res.append(new_str[i])
        if min_len < len(new_str):
            res.extend(new_str[min_len:])
        return "".join(res)
    
    if old_str == None or text == None:
        return text
    if new_str == None:
        new_str = ""
    if whole_words:
        old_str = "\\b" + old_str + "\\b"
    # Pro hledani celych slov potrebujeme na zacatek a konec old_str pridat metaznak \b (word boundary, je nutne escapovat zpetne lomitko!)
    return re.sub(old_str, f_match_case, text, flags=re.I)


if __name__ == "__main__":
    write_debug_output = False
    # Z parametru nacteme nazev souboru se SQL kodem a pozadovane kodovani (prvni parametr obsahuje nazev skriptu)
    if len(sys.argv) > 2:
        source_sql = str(sys.argv[1])
        encoding = str(sys.argv[2])
    else:
        # # Pokud bylo zadano malo parametru, zobrazime napovedu a ukoncime provadeni skriptu
        # print("\nSyntaxe:\n\n  sql2xml SOUBOR KODOVANI\n\nkde:\n  SOUBOR    cesta k souboru s SQL dotazem\n  KODOVANI  kódování, které má být použito při čtení souboru výše\n            (ansi, utf-8, utf-8-sig apod.)\n")
        # os._exit(1)  # sys.exit(1) vyvola dalsi vyjimku (SystemExit)!

        write_debug_output = True
        # DEBUG
        # source_sql = "./test-files/_subselect_v_operaci__utf8.sql"
        # source_sql = "./test-files/EI_znamky_2F_a_3F__utf8.sql"
        # source_sql = "./test-files/Plany_prerekvizity_kontrola__utf8.sql"
        # source_sql = "./test-files/Predmety_aktualni_historie__utf8.sql"
        # source_sql = "./test-files/Predmety_aktualni_historie_MOD__utf8.sql"
        # source_sql = "./test-files/sql_parse_pokus__utf8.sql"
        # encoding = "utf-8"
        # source_sql = "./test-files/PHD_studenti_SDZ_SZZ_predmety_publikace__utf8-sig.sql"
        # source_sql = "./test-files/PHD_studenti_SDZ_SZZ_predmety_publikace_MOD__utf8-sig.sql"
        # source_sql = "./test-files/Predmety_literatura_pouziti_v_planech_Apollo__utf8-sig.sql"
        # source_sql = "./test-files/Profese_Pridelene_AD_vymazat_orgunitu__utf8-sig.sql"
        # source_sql = "./test-files/Profese_Pridelene_AD_vymazat_orgunitu_MOD_WHERE_EXISTS__utf8-sig.sql"
        # source_sql = "./test-files/Program_garant_pocet_programu_sloucenych__utf8-sig.sql"
        # source_sql = "./test-files/Rozvrh_vyucovani_nesloucene_mistnosti_Apollo__utf8-sig.sql"
        # source_sql = "./test-files/Rozvrh_vyucovani_nesloucene_mistnosti_Apollo_MOD__utf8-sig.sql"
        # source_sql = "./test-files/Zav_prace_predb_zad_garanta.sql"
        # source_sql = "./test-files/_Funkce_Table_2_Row_ListAgg.sql"
        # source_sql = "./test-files/Absolventi_zdvojeny_titul_dve_studia_rekurze_test.sql"
        # source_sql = "./test-files/Absolventi_zdvojeny_titul_dve_studia_rekurze.sql"
        # source_sql = "./test-files/Absolventi_zdvojeny_titul_dve_studia_rekurze_clean.sql"
        # source_sql = "./test-files/Ankety_Studis_01_Seznam_anket.sql"
        # source_sql = "./test-files/Dodatek_zmena_kreditu_dle_planu.sql"
        # source_sql = "./test-files/Dodatek_zmena_kreditu_dle_planu_predmety.sql"
        # source_sql = "./test-files/EI_oprava_zapoctu.sql"
        # source_sql = "./test-files/EI_zapis_hodnoceni_A2_letni_kurz.sql"
        # source_sql = "./test-files/EI_zapis_hodnoceni_A2_letni_kurz_vyuka.sql"
        # source_sql = "./test-files/Evidence_chybne_poradi_oprava_01_mazani.sql"
        # source_sql = "./test-files/FIT_registrace_predmetu.sql"
        # source_sql = "./test-files/FIT_registrace_predmetu_simulace.sql"
        # source_sql = "./test-files/individualni_plan_fekt_func_01_orig.sql"
        # source_sql = "./test-files/IP_Doplneni_povinnosti_v_NMS_z_BS.sql"
        # source_sql = "./test-files/IP_jazyk_gener_NMS_oprava_spatne_nagener.sql"
        # source_sql = "./test-files/IP_kredity_Predmety_planu_oprava_poslat02.sql"
        # source_sql = "./test-files/IP_oprava_chybejici_navazujici_jazyky.sql"
        # source_sql = "./test-files/IP_pocty_predmetu_rozvolneni_rozvrhy_st_abs.sql"
        # source_sql = "./test-files/IP_predmet_skupina_osob.sql"
        # source_sql = "./test-files/IP_predmet_skupina_osob_with.sql"
        # source_sql = "./test-files/IP_PV_odstraneni_nadbytecnych_dle_casu_reg.sql"
        # source_sql = "./test-files/IP_registrace_PV_malo_moc_predmetu_jina_studia.sql"
        # source_sql = "./test-files/IP_registrace_zahranicni_stud.sql"
        # source_sql = "./test-files/Organizace_seznam_zahr_skol_pro_eprihlasku_Erasmus.sql"
        # source_sql = "./test-files/Person_aut_vztahy_externistum_s_dohodou.sql"
        # source_sql = "./test-files/PHD_studenti_aktivni_vazba_na_ustav.sql"
        # source_sql = "./test-files/Plany_perekvizity_kontrola_test.sql"
        # source_sql = "./test-files/Predmety_klony_kopirovani_karty.sql"
        # source_sql = "./test-files/Predmety_klony_kopirovani_karty_CVIS_3.sql"
        # source_sql = "./test-files/Prihlaska_LA_pocet_predmetu.sql"
        # source_sql = "./test-files/Rizeni_a_zadosti_Predmet_Zrusit.sql"
        # source_sql = "./test-files/Rozvrh_vytizeni_studentu_predmetu.sql"
        # source_sql = "./test-files/Rozvrh_vyucovani_nedostatecne_kapacity_s_skup_reg.sql"
        # source_sql = "./test-files/Rozvrh_vyucovani_nesloucene_mistnosti_Apollo_test.sql"
        # source_sql = "./test-files/Studenti_recyklanti_do_1.r.sql"
        # source_sql = "./test-files/Studis_PV_volba_insert.sql"
        # source_sql = "./test-files/Studis_PV_volba_prerekvizity_check.sql"
        # source_sql = "./test-files/Studis_PV_volba_prerekvizity_check_debug.sql"
        # source_sql = "./test-files/T2_Hodnoceni_studenta_predmety_03_bind.sql"
        # source_sql = "./test-files/T2_Hodnoceni_studenta_predmety_orig.sql"
        # source_sql = "./test-files/Temata_PHD_skolitele_pocty.sql"
        # source_sql = "./test-files/Terminy_kolidujici_pocty_studentu_hodnoceni_prehled.sql"
        # source_sql = "./test-files/Terminy_registrace_existuje_evidence_chybi_oprava.sql"
        source_sql = "./test-files/Terminy_registrace_poradi_evidence.sql"  # DORESIT -- komentarem rozdeleny "WITH ( SELECT ... )"
        source_sql = "./test-files/Terminy_registrace_poradi.sql"  # DORESIT -- komentarem rozdeleny "WITH ( SELECT ... )"
        # source_sql = "./test-files/TMP210_profese_t1_new.sql"
        # source_sql = "./test-files/TMP210_profese_t2_new.sql"
        # source_sql = "./test-files/TMP216_Bind_1.sql"
        # source_sql = "./test-files/TMP216_Bind_2.sql"
        # source_sql = "./test-files/TMP216_Prerekvizity_bind_ukazka.sql"
        # source_sql = "./test-files/Ucty_organizaci.sql"
        source_sql = "./test-files/Vizitky_mistnost_id_doplneni.sql"  # DORESIT -- rozdeleny WITH blok "[mistnosti as (] [...]" -- pomohlo by pridat mezeru pred "SELECT"?
        # source_sql = "./test-files/Zav_prace_predb_zad_seznam_teacher_nezprac.sql"
        source_sql = "./test-files/Zav_prace_prehled_praci_souboru_Apollo.sql"
        encoding = "utf-8-sig"
        # source_sql = "./test-files/Plany_prerekvizity_kontrola__ansi.sql"
        # source_sql = "./test-files/Predmety_planu_zkouska_projekt_vypisovani_vazba_err__ansi.sql"
        # encoding = "ansi"

    exit_code = 0
    fTxt = None
    fDia = None
    fNamePrefix = source_sql[:-4]
    warnings = []
    # BUG: pokud je slovo "result" pouzito jako nazev bloku ve WITH, ve vyctu v SELECT, FROM, JOIN, prip. v jinem podobnem kontextu, dojde vlivem chyby v sqlparse k umelemu rozdeleni tokenu a jinak funkcni algoritmus tedy selze. Vytvorime si tedy slovnik znamych problematickych vyrazu, ktere pred zpracovanim SQL kodu nahradime nahodne generovanymi retezci. Po zpracovani zase ve vytvorenych vystupech vse nahradime zpet (vzdy hromadne, jelikoz i konflikt s XML strukturou .dia je nepravdepodobny). Skutecnost, ze tak ztratime informaci o malych/velkych pismenech (tedy ze napr. "result", "Result", "RESULT" apod. budou ve vystupech oznacovany pouze jako "result"), neni uplne podstatna.
    try:
        print()
        with open(source_sql, mode="r", encoding=encoding) as file:
            query = "".join(file.readlines())

        # VYPSANI PUVODNIHO DOTAZU V PREFORMATOVANEM STAVU -- POZOR: FORMATOVANI SLOZITEJSICH SQL DOTAZU MNOHDY TRVA DELSI DOBU!
        # S komentari neni idealni (nektera zalomeni radku jsou orezana apod.)
        # print(f"\nPŘEFORMÁTOVANÝ DOTAZ (s komentáři):\n-----------------------------------\n{format(query, encoding=encoding, reindent=True, keyword_case='upper', strip_comments=False)}\n")
        # # Bez komentaru
        # formatted_sql = f"\nPŘEFORMÁTOVANÝ DOTAZ (bez komentářů):\n-------------------------------------\n{format(query, encoding=encoding, reindent=True, keyword_case='upper', strip_comments=True)}\n-------------------------------------\n"
        # print(formatted_sql)
        # # DEBUG: obcas se hodi ukladat vystup konzoly i na disk...
        if write_debug_output:
            fTxt = open(fNamePrefix + "_vystup.txt", mode="w", encoding="utf-8")
            # fTxt.write(formatted_sql + "\n")

        # Pred analyzou SQL kodu musime provest nahradu problematickych vyrazu. Nektere upravime jen kosmeticky (ale tak, ze se tim vyhneme problemum s parserem), jine je potreba nahradit nahodnymi retezci. Nejprve vyresime kosmeticke zmeny, pote stejny slovnik (replacements) vycistime a pouzijeme ho pro ulozeni docasnych nahrad. Zde na zacatku slovnik vyplnime problematickymi vyrazy, jako hodnotu lze ulozit pripadny nutny prefix nahodneho retezce. Potrebujeme, aby:
        #   * nahradni retezce byly ruzne,
        #   * v zadnem z nich se nevyskytoval lib. z puvodnich problematickych vyrazu a
        #   * zadny z nich nebyl v puvodnim SQL kodu (bez ohledu na velikost pismen).
        # Pro snazsi kontrolu si budeme generovane nahradni retezce ukladat do kolekce generated_r_strs.
        # Kompletni zachovani velikosti pismen je zarizno pomoci metody replace_keep_case(...), kde jsou velikosti pismen v nahradnim retezci nastaveny podle velikosti pismen v puvodnim vyrazu.
        # (a) Kosmeticke upravy (zavorku v klici je nutno escapovat, jelikoz hledame pomoci regularnich vyrazu!)
        replacements = {}
        # Nezlomitelne mezery nahradime standardnimi mezerami
        replacements["u\xa0"] = " "
        # Za kazdym klicovym slovem OVER potrebujeme mezeru, abychom se vyhnuli dalsi zbytecne uprave kodu pro podchyceni pripadu "OVER(". Nelze ale nahrazovat cela slova (s "word boundary" na zacatk ua konci regex retezce), takze musime pro predejiti potizim nahrazovat retezec vc. pocatecni mezery (resp. \n, \t).
        replacements[" over\\("] = " over ("
        replacements["\\nover\\("] = "\nover ("
        replacements["\\tover\\("] = "\tover ("
        # Podobne vyresime hinty "use_hash" a "use_nl"
        replacements["\\+use_hash\\("] = "+ use_hash ("
        replacements[" use_hash\\("] = " use_hash ("
        replacements["\\nuse_hash\\("] = "\nuse_hash ("
        replacements["\\tuse_hash\\("] = "\tuse_hash ("
        replacements["\\+use_nl\\("] = "+ use_nl ("
        replacements[" use_nl\\("] = " use_nl ("
        replacements["\\nuse_nl\\("] = "\nuse_nl ("
        replacements["\\tuse_nl\\("] = "\tuse_nl ("
        # COUNT(...) musi byt bez mezery, jinak neni vraceno jako funkce, ale jako samostatne klicove slovo
        replacements[" count \\("] = " count("
        replacements[",count \\("] = ", count("
        replacements["\\(count \\("] = "( count("
        replacements["\\ncount \\("] = "\ncount("
        replacements["\\tcount \\("] = "\tcount("
        # Za kazdou uzaviraci zavorkou potrebujeme mezeru, abychom podchytili pripad "funkce()alias" (toto by cele bylo vraceno jako jmeno, alias by chybel)
        replacements["\\)"] = ") "
        # # Podobne, byt uz ciste z kosmetickych duvodu vzhledem k nahradam zavorek, upravime vyskyty carek.
        # replacements["\\,"] = ", "
        r_keys = replacements.keys()
        for r_key in r_keys:
            # Nahrazujeme libovolne vyskyty (tzn. bez ohledu na to, zda je shoda nalezena jako cele slovo)
            query = replace_match_case(r_key, replacements[r_key], query, whole_words=False)

        # (b) Docasne nahrady nahodnymi retezci, boolovska hodnota znaci, zda pozadujeme nahradu celych slov
        replacements = {}
        replacements["data"] = ("", True)
        replacements["result"] = ("", True)
        replacements["rownum"] = ("", True)
        replacements["&&"] = (":", False)  # | Zde nas zachovani malych/velkych pismen netrapi, jelikoz nahrazujeme
        replacements["&"] = (":", False)   # | pouze ampersandy (napr. "&data" --> ":RANDOMSTRINGdata")

        # TODO: slo by takto vyresit i bug "within group"? (vyzadovalo by ale regex klic atd., jelikoz mezi slovy mohou byt bile znaky!)

        lc_query = query.lower()
        generated_r_strs = []
        r_keys = replacements.keys()
        for r_key in r_keys:
            (prefix, whole_words) = replacements[r_key]
            while True:
                # Potrebujeme retezec dost dlouhy na to, abychom nahodou nevygenerovali klicove slovo apod. (24 znaku by snad melo stacit). Zaroven musi jeho delka byt prinejmensim takova, jako delka puvodniho vyrazu, aby bylo mozne kompletne zachovat velikosti pismen. Pred nahodny retezec musime pridat prefix ulozeny ve slovniku replacements!
                r_str = prefix + get_random_string(max(24, len(r_key)))
                if r_str in generated_r_strs or r_str in lc_query:
                    continue
                key_in_r_str = False
                for rk in r_keys:
                    if rk in r_str:
                        key_in_r_str = True
                        break
                if not key_in_r_str:
                    generated_r_strs.append(r_str)
                    replacements[r_key] = (r_str, whole_words)
                    break
        # Nahradni retezce jsou nachystane, upravime obsah promenne query
        for r_key in r_keys:
            # Bezny string neumi case-insensitive nahrady --> musime provest pomoci modulu re
            (r_str, whole_words) = replacements[r_key]
            query = replace_match_case(r_key, r_str, query, whole_words=whole_words)

        # Nyni muzeme zacit parsovat
        statements = parse(query, encoding=encoding)
        for s in statements:
            process_statement(s)
        
        # Po zpracovani kodu je nutne provest zpetnou nahradu vsech drive nahrazenych problematickych vyrazu
        for r_key in replacements.keys():
            (r_str, whole_words) = replacements[r_key]
            for table in Table.__tables__:
                # Jmeno
                table.name = replace_match_case(r_str, r_key, table.name, whole_words=whole_words)
                # Aliasy (nutno iterovat pomoci indexu!)
                for key in table.statement_aliases.keys():
                    aliases = table.statement_aliases[key]
                    for i in range(len(aliases)):
                        aliases[i] = replace_match_case(r_str, r_key, aliases[i], whole_words=whole_words)
                # Atributy
                for attr in table.attributes:
                    # Jmeno
                    attr.name = replace_match_case(r_str, r_key, attr.name, whole_words=whole_words)
                    # Kratke jmeno
                    if attr.short_name != None:
                        attr.short_name = replace_match_case(r_str, r_key, attr.short_name, whole_words=whole_words)
                    # Alias
                    if attr.alias != None:
                        attr.alias = replace_match_case(r_str, r_key, attr.alias, whole_words=whole_words)
                    # # Podminky nemusime u standardnich atributu vubec resit, protoze je vzdy None
                    # if attr.condition != None:
                    #     attr.condition = replace_keep_case(repl_str, repl_key, attr.condition)
                    # Komentar
                    if attr.comment != None:
                        attr.comment = replace_match_case(r_str, r_key, attr.comment, whole_words=whole_words)
                # Podminky
                for attr in table.conditions:
                    attr.name = replace_match_case(r_str, r_key, attr.name, whole_words=whole_words)
                    if attr.short_name != None:
                        attr.short_name = replace_match_case(r_str, r_key, attr.short_name, whole_words=whole_words)
                    if attr.alias != None:
                        attr.alias = replace_match_case(r_str, r_key, attr.alias, whole_words=whole_words)
                    if attr.condition != None:
                        attr.condition = replace_match_case(r_str, r_key, attr.condition, whole_words=whole_words)
                    if attr.comment != None:
                        attr.comment = replace_match_case(r_str, r_key, attr.comment, whole_words=whole_words)
                # Bindovane promenne (nutno iterovat pomoci indexu!)
                for i in range(len(table.used_bind_vars)):
                    # Pred nahradou zpet musime nejprve pridat ":" a potom zase ":" (prip. jeden ci dva "&") zase odebrat!
                    bind_var = ":" + table.used_bind_vars[i]
                    bind_var = replace_match_case(r_str, r_key, bind_var, whole_words=whole_words)
                    while bind_var.startswith(":") or bind_var.startswith("&"):
                        bind_var = bind_var[1:]
                    table.used_bind_vars[i] = bind_var
                # Komentar
                if table.comment != None:
                    table.comment = replace_match_case(r_str, r_key, table.comment, whole_words=whole_words)
                # Zdrojovy kod
                if table.source_sql != None:
                    table.source_sql = replace_match_case(r_str, r_key, table.source_sql, whole_words=whole_words)

        # Byla v kodu nalezena alespon jedna tabulka? Pokud ne, vypiseme chybu pomoci vyjimky
        if len(Table.__tables__) == 0:
            raise Exception("Ve zdrojovem SQL souboru nebyla nalezena žádná tabulka")

        # Vypiseme textovou reprezentaci tabulek
        std_table_collection = []
        for table in Table.__tables__:
            # Jmena tabulek z DB si pouze ulozime do kolekce pro potreby pozdejsiho vypisu seznamu
            if table.table_type == Table.STANDARD_TABLE:
                std_table_collection.append(f"    * {table.name}")
            if write_debug_output:
                output = f"{table}\n"
                # # DEBUG: vypisy zatim zakazeme, aby slo lepe sledovat potencialne problematicka klicova slova
                # # Do konzoly vypiseme tabulky z WITH, mezi-tabulky vypisovat nebudeme
                # if table.table_type == Table.WITH_TABLE:
                #     print(output)
                # DEBUG: ulozeni textove reprezentace tabulek do souboru (pro potreby ladeni)
                fTxt.write(output + "\n")

        if len(std_table_collection) > 0:
            std_table_collection.sort()
            output = "\nTento SQL dotaz používá následující tabulky z DB:\n" + "\n".join(std_table_collection) + "\n"
        else:
            output = "\nTento SQL dotaz nepoužívá žádné tabulky z DB.\n"
        print(output)
        if write_debug_output:
            fTxt.write(output)

        # # Bloky a vazby mezi nimi ulozime v XML formatu kompatibilnim s aplikaci Dia ( https://wiki.gnome.org/Apps/Dia )
        header = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                  "<dia:diagram xmlns:dia=\"http://www.lysator.liu.se/~alla/dia/\">\n"
                  "  <dia:diagramdata>\n"
                  "    <dia:attribute name=\"background\">\n"
                  "      <dia:color val=\"#ffffff\"/>\n"
                  "    </dia:attribute>\n"
                  "    <dia:attribute name=\"pagebreak\">\n"
                  "      <dia:color val=\"#000099\"/>\n"
                  "    </dia:attribute>\n"
                  "    <dia:attribute name=\"paper\">\n"
                  "      <dia:composite type=\"paper\">\n"
                  "        <dia:attribute name=\"name\">\n"
                  "          <dia:string>#A4#</dia:string>\n"
                  "        </dia:attribute>\n"
                  "        <dia:attribute name=\"tmargin\">\n"
                  "          <dia:real val=\"0.50\"/>\n"
                  "        </dia:attribute>\n"
                  "        <dia:attribute name=\"bmargin\">\n"
                  "          <dia:real val=\"0.50\"/>\n"
                  "        </dia:attribute>\n"
                  "        <dia:attribute name=\"lmargin\">\n"
                  "          <dia:real val=\"0.50\"/>\n"
                  "        </dia:attribute>\n"
                  "        <dia:attribute name=\"rmargin\">\n"
                  "          <dia:real val=\"0.50\"/>\n"
                  "        </dia:attribute>\n"
                  "        <dia:attribute name=\"is_portrait\">\n"
                  "          <dia:boolean val=\"false\"/>\n"
                  "        </dia:attribute>\n"
                  "        <dia:attribute name=\"scaling\">\n"
                  "          <dia:real val=\"1\"/>\n"
                  "        </dia:attribute>\n"
                  "        <dia:attribute name=\"fitto\">\n"
                  "          <dia:boolean val=\"true\"/>\n"
                  "        </dia:attribute>\n"
                  "      </dia:composite>\n"
                  "    </dia:attribute>\n"
                  "    <dia:attribute name=\"grid\">\n"
                  "      <dia:composite type=\"grid\">\n"
                  "        <dia:attribute name=\"width_x\">\n"
                  "          <dia:real val=\"1\"/>\n"
                  "        </dia:attribute>\n"
                  "        <dia:attribute name=\"width_y\">\n"
                  "          <dia:real val=\"1\"/>\n"
                  "        </dia:attribute>\n"
                  "        <dia:attribute name=\"visible_x\">\n"
                  "          <dia:int val=\"1\"/>\n"
                  "        </dia:attribute>\n"
                  "        <dia:attribute name=\"visible_y\">\n"
                  "          <dia:int val=\"1\"/>\n"
                  "        </dia:attribute>\n"
                  "        <dia:composite type=\"color\"/>\n"
                  "      </dia:composite>\n"
                  "    </dia:attribute>\n"
                  "    <dia:attribute name=\"color\">\n"
                  "      <dia:color val=\"#d8e5e5\"/>\n"
                  "    </dia:attribute>\n"
                  "    <dia:attribute name=\"guides\">\n"
                  "      <dia:composite type=\"guides\">\n"
                  "        <dia:attribute name=\"hguides\"/>\n"
                  "        <dia:attribute name=\"vguides\"/>\n"
                  "      </dia:composite>\n"
                  "    </dia:attribute>\n"
                  "  </dia:diagramdata>\n"
                  "  <dia:layer name=\"Background\" visible=\"true\" active=\"true\">\n")
        footer = ("  </dia:layer>\n"
                  "</dia:diagram>\n")
        # Text je zobrazen vzdy cerne, ale samotne tabulky jsou barevne odlisene podle druhu
        # # Barva beznych tabulek (Table.STANDARD_TABLE)  # TODO: aktualne neni potreba
        # std_bg_color = "EEEEEE"
        # Barva tabulek ve WITH (Table.WITH_TABLE) s .uses_bind_vars() == False
        with_bg_color = "FEE79C"
        # Barva obrysu tabulek ve WITH (Table.WITH_TABLE) s .uses_bind_vars() == True
        with_bind_fg_color = "2E4DE6"
        # Barva SLECTu na nejvyssi urovni (Table.MAIN_SELECT)
        ms_bg_color = "EC6964"
        fDia = gzip.open(filename=fNamePrefix+".dia", mode="wb", compresslevel=9)
        fDia.write(bytes(header, "UTF-8"))
        # Okraj uvazovany pri vypoctu bounding boxu (== polovina line_width v kodu nize, coz staci mit napevno)
        bb = 0.05
        # Bloky budeme rozmistovat do matice s max. poctem bloku na jednom "radku" n_blocks_h, pricemz bloky nejvice vlevo budou na horiz. pozici x0
        n_blocks_h = 10
        x0 = bb
        # Pozice nasledujiciho bloku
        x = x0
        y = bb
        # Rozmery bloku
        w = 10
        h = 5
        # Horiz./vert. posuny pro umistovani propojeni bloku (blok nemusi byt s ohledem na uvedeny text po otevreni siroky "w" + sipka vede z/do mista v polovine zahlavi s nazvem)
        dw = 0.3 * w
        dh = 0.1 * h
        # Posun dvou bloku vuci sobe (horiz./vert.)
        dx = w + 3
        dy = h + 3

        # Budeme vykreslovat pouze tabulky z WITH/SELECT na nejvyssi urovni. Aby bylo mozne spravne pridat zavislosti, musime nejprve u kazde takove tabulky zjistit, jestli "oklikou" (pres mezi-tabulku/y) nezavisi na jine tabulce z WITH. Takove zavislosti si opet ulozime do slovniku, kde klicem bude ID tabulky a hodnotou seznam ID navazanych tabulek.
        primary_linked_ids = {}
        for table in Table.__tables__:
            # Zavislosti budeme hledat vyhradne u tabulek z WITH bloku
            if (table.table_type == Table.WITH_TABLE
                    or table.table_type == Table.MAIN_SELECT):
                primary_linked_ids[table.id] = get_primary_linked_ids(table, path=[])

        # Nyni muzeme zacit "sazet" bloky na (jedinou) vrstvu v diagramu. Kod bloku budeme skladat postupne jako kolekci (aby slo snadno pouzivat f-strings) a az nakonec vse sloucime a zapiseme do souboru. Propojeni bloku pridame az pote, co budou veskere bloky v XML (k tomu si budeme do block_pos ukladat ID tabulek a jim odpovidajici pozice bloku).
        block_pos = {}
        # Pro generovani ID bloku budeme kvuli preskakovani vsech tabulek krome tech z WITH potrebovat novou promennou. Dale take bude nutny slovnik pro preklad ID tabulky na ID objektu.
        obj_id = -1
        table_id_to_obj_id = {}
        # Index nasl. bloku pouzivany pri rozmistovani na "radku"
        i = 0
        for table in Table.__tables__:
            # Preskocime vsechny tabulky, ktere nejsou primo z WITH bloku/SELECTy na nejvyssi urovni
            if (table.table_type != Table.WITH_TABLE
                    and table.table_type != Table.MAIN_SELECT):
                continue
            obj_id += 1
            table_id_to_obj_id[table.id] = obj_id
            block_pos[obj_id] = (x, y)
            code = []
            code.append(f"    <dia:object type=\"UML - Class\" version=\"0\" id=\"O{obj_id}\">\n")
            code.append( "      <dia:attribute name=\"obj_pos\">\n")
            code.append(f"        <dia:point val=\"{x},{y}\"/>\n")
            code.append(("      </dia:attribute>\n"
                         "      <dia:attribute name=\"obj_bb\">\n"))
            code.append(f"        <dia:rectangle val=\"{x - bb},{y - bb};{x + w + bb},{y + h + bb}\"/>\n")
            code.append(("      </dia:attribute>\n"
                         "      <dia:attribute name=\"elem_corner\">\n"))
            code.append(f"        <dia:point val=\"{x},{y}\"/>\n")
            code.append(("      </dia:attribute>\n"
                         "      <dia:attribute name=\"elem_width\">\n"))
            code.append(f"        <dia:real val=\"{w}\"/>\n")
            code.append(("      </dia:attribute>\n"
                         "      <dia:attribute name=\"elem_height\">\n"))
            code.append(f"        <dia:real val=\"{h}\"/>\n")
            code.append(("      </dia:attribute>\n"
                         "      <dia:attribute name=\"name\">\n"))
            code.append(f"        <dia:string>#{table.name}#</dia:string>\n")
            code.append(("      </dia:attribute>\n"
                         "      <dia:attribute name=\"stereotype\">\n"
                         "        <dia:string>##</dia:string>\n"
                         "      </dia:attribute>\n"
                         "      <dia:attribute name=\"comment\">\n"))
            code.append(f"        <dia:string>#{text_to_dia(table.comment)}#</dia:string>\n")
            code.append(("      </dia:attribute>\n"
                         "      <dia:attribute name=\"abstract\">\n"
                         "        <dia:boolean val=\"false\"/>\n"
                         "      </dia:attribute>\n"
                         "      <dia:attribute name=\"suppress_attributes\">\n"
                         "        <dia:boolean val=\"false\"/>\n"
                         "      </dia:attribute>\n"
                         "      <dia:attribute name=\"suppress_operations\">\n"
                         "        <dia:boolean val=\"false\"/>\n"
                         "      </dia:attribute>\n"
                         "      <dia:attribute name=\"visible_attributes\">\n"
                         "        <dia:boolean val=\"false\"/>\n"
                         "      </dia:attribute>\n"
                         "      <dia:attribute name=\"visible_operations\">\n"
                         "        <dia:boolean val=\"false\"/>\n"
                         "      </dia:attribute>\n"
                         "      <dia:attribute name=\"visible_comments\">\n"
                         "        <dia:boolean val=\"true\"/>\n"
                         "      </dia:attribute>\n"
                         "      <dia:attribute name=\"wrap_operations\">\n"
                         "        <dia:boolean val=\"true\"/>\n"
                         "      </dia:attribute>\n"
                         "      <dia:attribute name=\"wrap_after_char\">\n"
                         "        <dia:int val=\"30\"/>\n"
                         "      </dia:attribute>\n"
                         "      <dia:attribute name=\"comment_line_length\">\n"
                         "        <dia:int val=\"40\"/>\n"
                         "      </dia:attribute>\n"
                         "      <dia:attribute name=\"comment_tagging\">\n"
                         "        <dia:boolean val=\"false\"/>\n"
                         "      </dia:attribute>\n"
                         "      <dia:attribute name=\"line_width\">\n"
                         "        <dia:real val=\"0.1\"/>\n"
                         "      </dia:attribute>\n"
                         "      <dia:attribute name=\"line_color\">\n"))
            # Vykreslujeme pouze tab. z WITH, resp. SELECT na nejvyssi urovni --> barvy lze nastavit obycejnym IF ... ELSE ...
            if table.uses_bind_vars():
                color = f"        <dia:color val=\"#{with_bind_fg_color}\"/>\n"
            else:
                color = f"        <dia:color val=\"#000000\"/>\n"
            code.append(color)
            code.append(("      </dia:attribute>\n"
                         "      <dia:attribute name=\"fill_color\">\n"))
            if table.table_type == Table.WITH_TABLE:
                color = f"        <dia:color val=\"#{with_bg_color}\"/>\n"
            else:
                color = f"        <dia:color val=\"#{ms_bg_color}\"/>\n"
            # elif table.table_type == Table.AUX_TABLE:
            #     bg_color = f"        <dia:color val=\"#{aux_bg_color}\"/>\n"
            # else:
            #     bg_color = f"        <dia:color val=\"#{std_bg_color}\"/>\n"
            code.append(color)
            code.append(("      </dia:attribute>\n"
                         "      <dia:attribute name=\"text_color\">\n"
                         "        <dia:color val=\"#000000\"/>\n"
                         "      </dia:attribute>\n"
                         "      <dia:attribute name=\"normal_font\">\n"
                         "        <dia:font family=\"monospace\" style=\"0\" name=\"Courier\"/>\n"
                         "      </dia:attribute>\n"
                         "      <dia:attribute name=\"abstract_font\">\n"
                         "        <dia:font family=\"monospace\" style=\"0\" name=\"Courier\"/>\n"
                         "      </dia:attribute>\n"
                         "      <dia:attribute name=\"polymorphic_font\">\n"
                         "        <dia:font family=\"monospace\" style=\"0\" name=\"Courier\"/>\n"
                         "      </dia:attribute>\n"
                         "      <dia:attribute name=\"classname_font\">\n"
                         "        <dia:font family=\"sans\" style=\"80\" name=\"Helvetica-Bold\"/>\n"
                         "      </dia:attribute>\n"
                         "      <dia:attribute name=\"abstract_classname_font\">\n"
                         "        <dia:font family=\"sans\" style=\"0\" name=\"Helvetica\"/>\n"
                         "      </dia:attribute>\n"
                         "      <dia:attribute name=\"comment_font\">\n"
                         "        <dia:font family=\"sans\" style=\"4\" name=\"Helvetica\"/>\n"
                         "      </dia:attribute>\n"
                         "      <dia:attribute name=\"normal_font_height\">\n"
                         "        <dia:real val=\"0.80\"/>\n"
                         "      </dia:attribute>\n"
                         "      <dia:attribute name=\"polymorphic_font_height\">\n"
                         "        <dia:real val=\"0.80\"/>\n"
                         "      </dia:attribute>\n"
                         "      <dia:attribute name=\"abstract_font_height\">\n"
                         "        <dia:real val=\"0.80\"/>\n"
                         "      </dia:attribute>\n"
                         "      <dia:attribute name=\"classname_font_height\">\n"
                         "        <dia:real val=\"1\"/>\n"
                         "      </dia:attribute>\n"
                         "      <dia:attribute name=\"abstract_classname_font_height\">\n"
                         "        <dia:real val=\"1\"/>\n"
                         "      </dia:attribute>\n"
                         "      <dia:attribute name=\"comment_font_height\">\n"
                         "        <dia:real val=\"0.70\"/>\n"
                         "      </dia:attribute>\n"
                         "      <dia:attribute name=\"attributes\">\n"))
            # Podkomentar (ulozime pouze v pripade, ze existuje)
            if table.subcomment != None and len(table.subcomment) > 0:
                code.append(generateDiaBlockAttrCode("Podkomentář", text_to_dia(table.subcomment)))
            # Aliasy (ulozime pouze v pripade, ze existuje alespon jeden)
            aliases = Table.get_all_known_aliases(table.id)
            if len(aliases) > 0:
                # Aliasy chceme mit serazene podle abecedy
                aliases.sort()
                code.append(generateDiaBlockAttrCode("Aliasy", ", ".join(aliases)))
            # Atributy
            if len(table.attributes) > 0:
                attributes = []
                for attr in table.attributes:
                    name = Table.__trim_to_length__(attr.name)
                    if attr.alias != None:
                        attributes.append(f"{name} as {attr.alias}")
                    else:
                        attributes.append(f"{name}")
                    # Pridame pocatecni odrazku/hvezdicku (.join(...) tyto samozrejme prida jen mezi jednotlive atributy...)
                attributes[0] = attributes[0]
                code.append(generateDiaBlockAttrCode("Sloupce", "\n".join(attributes)))
            # Bindovane promenne
            if table.uses_bind_vars():
                table.used_bind_vars.sort()
                code.append(generateDiaBlockAttrCode("Bindované proměnné", ", ".join(table.used_bind_vars)))
            # SQL kod
            if table.source_sql != None and len(table.source_sql) > 0:
                code.append(generateDiaBlockAttrCode("SQL kód", table.source_sql))
            code.append(("      </dia:attribute>\n"
                         "      <dia:attribute name=\"operations\"/>\n"
                         "      <dia:attribute name=\"template\">\n"
                         "        <dia:boolean val=\"false\"/>\n"
                         "      </dia:attribute>\n"
                         "      <dia:attribute name=\"templates\"/>\n"
                         "    </dia:object>\n"))
            # Blok zapiseme do vystupniho souboru
            fDia.write(bytes("".join(code), "UTF-8"))
            # Nakonec aktualizujeme souradnice nasledujiciho bloku tak, aby bloky byly rozmisteny v matici o pozadovanem poctu sloupcu
            i += 1
            if i < n_blocks_h:
                x += dx
            else:
                i = 0
                x = x0
                y += dy

        # Po vlozeni vsech bloku muzeme pridat propojeni mezi nimi (priblizne souradnice budeme dopocitavat na zaklade pozic bloku)
        for table in Table.__tables__:
            # Opet preskocime vsechny tabulky, ktere nejsou primo z WITH bloku/SELECTy na nejvyssi urovni
            if (table.table_type != Table.WITH_TABLE
                    and table.table_type != Table.MAIN_SELECT):
                continue
            # Je tabulka navazana na alespon jednu jinou tabulkou?
            table_linked_to_primary_ids = primary_linked_ids[table.id]
            if len(table_linked_to_primary_ids) > 0:
                current_block_id = table_id_to_obj_id[table.id]
                # Pozice akt. tabulky
                (bx, by) = block_pos[current_block_id]
                for id in table_linked_to_primary_ids:
                    linked_block_id = table_id_to_obj_id[id]
                    # Pozice navazane (zdrojove) tabulky
                    (sx, sy) = block_pos[linked_block_id]
                    # Nachystame si ID objektu a muzeme zacit generovat kod
                    obj_id += 1
                    code = []
                    code.append(f"    <dia:object type=\"Standard - PolyLine\" version=\"0\" id=\"O{obj_id}\">\n")
                    code.append( "      <dia:attribute name=\"obj_pos\">")
                    # Koncove body a bounding box umistime jen priblizne -- Dia si stejne po otevreni souboru vse doladi
                    x_min = min(bx + dw, sx)
                    x_max = max(bx + dw, sx)
                    y_min = min(by + dh, sy + dh)
                    y_max = max(by + dh, sy + dh)
                    code.append(f"        <dia:point val=\"{x_min},{y_min}\"/>\n")
                    code.append(("      </dia:attribute>\n"
                                 "      <dia:attribute name=\"obj_bb\">\n"))
                    code.append(f"        <dia:rectangle val=\"{x_min - bb},{y_min - bb};{x_max + bb},{y_max + bb}\"/>\n")
                    code.append(("      </dia:attribute>\n"
                                 "      <dia:attribute name=\"poly_points\">"))
                    code.append(f"        <dia:point val=\"{bx + dw},{by + dh}\"/>\n")
                    code.append(f"        <dia:point val=\"{sx},{sy + dh}\"/>\n")
                    code.append(("      </dia:attribute>\n"
                                 "      <dia:attribute name=\"numcp\">\n"
                                 "        <dia:int val=\"1\"/>\n"
                                 "      </dia:attribute>\n"
                                 "      <dia:attribute name=\"end_arrow\">\n"
                                 "        <dia:enum val=\"22\"/>\n"
                                 "      </dia:attribute>\n"
                                 "      <dia:attribute name=\"end_arrow_length\">\n"
                                 "        <dia:real val=\"0.5\"/>\n"
                                 "      </dia:attribute>\n"
                                 "      <dia:attribute name=\"end_arrow_width\">\n"
                                 "        <dia:real val=\"0.5\"/>\n"
                                 "      </dia:attribute>\n"
                                 "      <dia:connections>\n"))
                    # Sipky chceme opacne, nez je zvykem v UML
                    # Pro korektni propojeni smerem ke stredum bloku jsou potreba ID poslednich connection pointu. tato ID proto musime dopocitat na zaklade poctu atributu:
                    #   * pro current_block_id toto zjistime primo z table
                    #   * pro linked_block_id musime najit danou tabulku pomoci Table.get_table_by_id(id)
                    # Dale take musime vzit v uvahu pripadnou rekurzi, kde spojujeme blok sam se sebou. V takovem pripade nelze spojovat jediny uzel, ale spojime connection pointy 4 a 3 (po stranach titulni casti bloku).
                    if current_block_id != linked_block_id:
                        # Ve vychozim nastaveni jsou atributy skryte, tzn. spojujeme primo uzly c. 8 (neni potreba dopocitavat ID stredoveho uzlu)
                        current_block_cp_id = 8  # + 2 * max(len(table.attributes), 1)
                        linked_block_cp_id = 8  # + 2 * max(len(Table.get_table_by_id(id).attributes), 1)
                    else:
                        current_block_cp_id = 3
                        linked_block_cp_id = 4
                    code.append(f"        <dia:connection handle=\"0\" to=\"O{linked_block_id}\" connection=\"{linked_block_cp_id}\"/>\n")
                    code.append(f"        <dia:connection handle=\"1\" to=\"O{current_block_id}\" connection=\"{current_block_cp_id}\"/>\n")
                    code.append(("      </dia:connections>\n"
                                 "    </dia:object>\n"))
                    # Blok zapiseme do vystupniho souboru
                    fDia.write(bytes("".join(code), "UTF-8"))

        #Uplne nakonec jeste musime zapsat koncovou cast XML
        fDia.write(bytes(footer, "UTF-8"))
    except:
        print("\nDOŠLO K CHYBĚ:\n\n" + traceback.format_exc())

        # DEBUG
        if write_debug_output:
            with open(fNamePrefix + "_CHYBA.txt", mode="w", encoding="utf-8") as error_file:
                error_file.write(traceback.format_exc())

        exit_code = 1
    finally:
        if fTxt != None:
            fTxt.close()
        if fDia != None:
            fDia.close()
        if write_debug_output and len(warnings) > 0:
            with open(fNamePrefix + "_VAROVANI.txt", mode="w", encoding="utf-8") as warning_file:
                warning_file.write("".join(warnings))
    os._exit(exit_code)  # sys.exit(exit_code) nelze s exit_code > 0 pouzit -- vyvola dalsi vyjimku (SystemExit)
