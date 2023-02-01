#!/usr/bin/python3

import sqlparse.sql as sql
from sqlparse import format, parse
from typing import Any
import sys
import os
import traceback


class Attribute:
    """Trida reprezentujici atribut (vc. pripadneho aliasu a pozadovane hodnoty)"""
    def __init__(self, name, alias=None, condition=None, comment=None):
        self.name = name
        self.alias = alias
        self.condition = condition
        if comment != None:
            comment = comment.strip()
        self.comment = comment


class Table:
    """Trida reprezentujici tabulku (kazda tabulka ma unikatni ID a jmeno)"""
    # Nejnizsi volne ID
    __next_id__ = 0
    # Mnozina sablon pro automatickou tvorbu nazvu tabulek (klic == sablona, hodnota == aktualni poradove cislo k pouziti pri tvorbe nazvu)
    __next_template_num__ = {}
    # # Vychozi schema uvazovane pri ukladani celych jmen tabulek
    # __default_schema__ = "st01"
    # Kolekce nalezenych tabulek
    __tables__ = []

    def __init__(self, name=None, name_template=None, alias=None, attributes=None, comment=None, source_sql=None):
        self.id = Table.__generate_id__()
        if name != None:
            # # Zkontrolujeme, zda mame cele jmeno tabulky (= vc. schematu) -- pokud ne, doplnime k nazvu vychozi schema
            # if not "." in name:
            #     name = f"{Table.__default_schema__}.{name}"
            self.name = name
        else:
            # Jmeno nebylo zadane, tzn. pracujeme s mezi-tabulkou, jejiz jmeno ulozime bez nazvu schematu
            self.name = Table.__generate_name__(name_template)
        self.aliases = []
        if alias != None:
            self.aliases.append(alias)
        self.attributes = []
        if attributes != None:
            self.attributes.extend(attributes)
        if comment != None:
            # Komentar ulozime bez zbytecnych leading/trailing whitespaces
            comment = comment.strip()
        self.comment = comment
        if source_sql != None:
            # SQL kod taktez ulozime bez leading/trailing whitespaces
            source_sql = source_sql.strip()
        self.source_sql = source_sql
        self.linked_to_tables_id = []

    def __str__(self) -> str:
        # Odsazeni pouzivane pri vypisu tabulek
        indent = "    "
        if len(self.aliases) > 0:
            # Aliasychceme mit serazene podle abecedy
            self.aliases.sort()
            aliases = f"\n{indent}{indent}".join(self.aliases)
        else:
            aliases = "<žádné>"
        if len(self.attributes) > 0:
            # Textove reprezentace atributu vc. pripadnych podminek, aliasu a komentaru nejprve ulozime do kolekce
            attribute_collection = []
            for attr in self.attributes:
                if attr.condition != None:
                    condition = f" {attr.condition}"
                else:
                    condition = ""
                if attr.alias != None:
                    alias = f" (alias: {attr.alias})"
                else:
                    alias = ""
                if attr.comment != None and len(attr.comment) > 0:
                    # Komentar muze byt dlouhy --> v takovem pripade vypiseme pouze jeho zacatek
                    attr_comment = f"\n{indent}{indent}{indent}Komentář: \"{Table.__trim_to_length__(attr.comment)}\""
                else:
                    attr_comment = ""
                attribute_collection.append(f"{Table.__trim_to_length__(attr.name)}{condition}{alias}{attr_comment}")
            # # Kolekci atributu nebudeme tridit podle abecedy (jednak chceme zachovat poradi atributu a jednak by to zpusobilo problemy v situaci, kdy je vytvarena pomocna tabulka s v kodu natvrdo zadanymi hodnotami)
            # attribute_collection.sort()
            attributes = f"\n{indent}{indent}".join(attribute_collection)
        else:
            attributes = "<žádné>"
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
        source_sql = Table.__trim_to_length__(self.source_sql)
        return f"TABULKA {self.name} (ID {self.id})\n{indent}Aliasy:\n{indent}{indent}{aliases}\n{indent}Atributy:\n{indent}{indent}{attributes}\n{indent}Vazba na tabulky:\n{indent}{indent}{names}\n{indent}Komentář:\n{indent}{indent}\"{comment}\"\n{indent}SQL kód:\n{indent}{indent}\"{source_sql}\""
    
    @classmethod
    def __trim_to_length__(cls, text: str) -> str:
        """Zkrati zadany text na max_snippet_length == 50 znaku, prip. vrati puvodni text, pokud byl kratsi. Veskera zalomeni radku, vicenasobne bile znaky apod. jsou zaroven nahrazeny jednotlivymi mezerami."""
        # Potreba v situaci, kdy napr. komentar k tabulce/atributu je None
        if text == None:
            return ""
        text = " ".join(text.split())
        max_snippet_length = 50
        if len(text) < max_snippet_length:
            return text
        else:
            return text[:(max_snippet_length - 6)] + " [...]"

    # @classmethod
    # def add_alias_to_table_name(cls, name, alias):
    #     # Pokud tabulka existuje, prida k ni alias; pokud naopak zatim neexistuje, vytvori novou tabulku s patricnym aliasem
    #     if name == alias:
    #         return False
    #     for tbl in Table.__tables__:
    #         if (name == tbl.name
    #                 or (name in tbl.aliases and alias != tbl.name)):
    #             return tbl.add_alias(alias)
    #     Table.__tables__.append(Table(name=name, alias=alias))
    #     return True

    @classmethod
    def add_alias_to_table(cls, id: int, alias: str) -> bool:
        """Zkusi pridat alias k tabulce zadaneho ID. Pokud toto probehne uspesne, vrati True, jinak vrati False"""
        if id < 0 or alias == None:
            return False
        for table in Table.__tables__:
            if id == table.id:
                return table.add_alias(alias)
        return False

    # @classmethod
    # def __get_canonical_name__(cls, name: str) -> str:
    #     """Vrati cele jmeno tabulky vc. pripadneho nazvu schematu. POZOR: metoda predpoklada, ze zadane jmeno je "pricetne" (= neni None/...)!"""
    #     if not "." in name:
    #         is_aux_name = False
    #         aux_names = Table.__next_template_num__.keys()
    #         for aname in aux_names:
    #             if name.startswith(f"{aname}-"):
    #                 is_aux_name = True
    #                 break
    #         # Zadane jmeno neobsahuje nazev schematu (protoze v nazvu neni tecka) a zaroven neodpovida zadnemu pouzitemu typu mezi-tabulky --> do nazvu doplnime schema
    #         if not is_aux_name:
    #             name = f"{Table.__default_schema__}.{name}"
    #     return name

    @classmethod
    def get_table_by_name(cls, name: str) -> "Table":
        """Vrati odkaz na tabulku zadaneho jmena, prip. None, pokud v kolekci Table.__tables__ zadna takova tabulka neexistuje. Porovnavani jmen je case-sensitive!"""
        if name == None:
            return None
        # # Jmeno tabulky musime porovnavat vc. pripadneho nazvu schematu (aliasy naopak porovnavame primo s tim, co mame zadano)
        # cname = Table.__get_canonical_name__(name)
        for table in Table.__tables__:
            if (name == table.name or name in table.aliases):
                return table
        return None

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
        """Vytvori jmeno tabulky podle zadane sablony (template). Pokud sablona neni zadana nebo jde pouze o sekvenci mezer, je jako sablona pouzit retezec "table". Jelikoz jsou generovana pouze jmena mezi-tabulek reprezentujicich SELECT apod., neobsahuji vracene retezce nazev schematu."""
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

    def add_alias(self, alias: str) -> bool:
        """Zkusi k tabulce pridat zadany alias. Pokud se toto podari, vrati True, jinak (napr. pokud uz zadany alias je mezi znamymi aliasy) vrati False. Porovnavani aliasu je case-sensitive + metoda NEKONTROLUJE pritomnost zadaneho aliasu u zbylych tabulek!"""
        # Zadany alias staci zkontrolovat bez prihlednuti k pripadnemu nazvu schematu
        if alias == None or self.name == alias or alias in self.aliases:
            return False
        self.aliases.append(alias)
        return True

    # def remove_alias(self, alias):
    #     try:
    #         self.aliases.remove(alias)
    #         return True
    #     except:
    #         return False

    def link_to_table_id(self, id: int) -> bool:
        """Nastavi vazbu aktualni tabulky na tabulku se zadanym ID. Pokud uz vazba existuje, vrati False, jinak vrati True."""
        if id in self.linked_to_tables_id:
            return False
        self.linked_to_tables_id.append(id)
        return True

    # def link_to_table_name(self, name: str):
    #     id = -1
    #     for table in Table.__tables__:
    #         if name == table.name or name in table.aliases:
    #             id = table.id
    #             break
    #     if id < 0 or id in self.linked_to_tables_id:
    #         return False
    #     self.linked_to_tables_id.append(id)
    #     return True

    # def unlink_from_table_id(self, id):
    #     try:
    #         self.linked_to_tables_id.remove(id)
    #         return True
    #     except:
    #         return False

    def update_attributes(self, attributes: list) -> None:
        """Aktualizuje jiz existujici atributy u tabulky podle zadane kolekce. Hledani (case-sensitive!) vzajemne odpovidajicich atributu je provadeno na zaklade jmen a aliasu."""
        # Kolekce, do ktere budeme ukladat atributy k pridani (urychli prohledavani kolekce jiz existujicich atributu)
        new_attributes = []
        for a in attributes:
            add_attrib = True
            # Pokud k atributu ze zadane kolekce najdeme jeho ekvivalent v jiz existujicich atributech, upravime jeho parametry podle atribitu ze zadane kolekce
            for ta in self.attributes:
                if (a.name == ta.name
                        or a.name == ta.alias
                        or a.alias == ta.name
                        or ((a.alias != None or ta.alias != None) and a.alias == ta.alias)):
                    ta.condition = a.condition
                    ta.comment = a.comment
                    add_attrib = False
                    break
            # Jestlize jsme ekvivalentni atribut nenasli, ulozime ten zadany do kolekce s atributy k pridani
            if add_attrib:
                new_attributes.append(a)
        # Nakonec pridame k tabulce vsechny nove atributy
        self.attributes.extend(new_attributes)


def is_comment(t: sql.Token) -> bool:
    """Vraci True/false podle toho, zda zadany token je SQL komentarem (tridu nestaci srovnavat jen s sql.Comment!)"""
    return (isinstance(t, sql.Comment)
        or t.ttype == sql.T.Comment.Single
        or t.ttype == sql.T.Comment.Multiline)


def get_name_alias_comment(t: sql.Token) -> tuple:
    """Extrahuje ze zadaneho tokenu jmeno a pripadny alias a komentar (typicke uziti: SELECT ... FROM <token>)"""
    # Struktura: name [ whitespace(s) [ AS whitespace(s) ] alias ]
    # kde "name" muze byt Identifier, prip. Function
    # Pocatecni tokeny v t.tokens jsou soucasti jmena (s pripadnymi oddelovaci/teckami) --> slozky jmena tedy ukladame do kolekce components tak dlouho, nez narazime na bily znak nebo komentar (ev. konec t.tokens)
    i = 0
    components = []
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
    # Nakonec jeste overime typ posledniho tokenu v t.tokens -- jde-li o komentar, vratime ho spolu se jmenem a aliasem (pripadne bile znaky za komentarem uz sqlparse nevraci jako soucast tokenu t). Z komentare neni potreba odstranovat leading/trailing whitespaces, jelikoz toto je provedeno  vkontruktoru.
    last_token = t.tokens[-1]
    if is_comment(last_token):
        return name, alias, last_token.value
    return name, alias, None


def process_comparison(t: sql.Comparison) -> Attribute:
    """Vraci atribut vc. pozadovane hodnoty (typicke uziti: ... JOIN ... ON <token>)"""
    # Pocatecni tokeny v t.tokens jsou soucasti jmena atributu (s pripadnymi oddelovaci/teckami) --> tyto ukladame do components
    components = []
    j = 0
    # Posledni cast podminky je nutna v pripade, ze pred operatorem neni mezera. POZOR: "...ttype != sql.T.Comparison" NENI TOTEZ JAKO "not isinstance(..., sql.Comparison)"!
    while j < len(t.tokens) and not t.tokens[j].is_whitespace and t.tokens[j].ttype != sql.T.Comparison:
        components.append(t.tokens[j].value)
        j += 1
    name = "".join(components)
    # Ted preskocime vse az po misto, kde se nachazi Comparison (operator)
    while j < len(t.tokens) and t.tokens[j].ttype != sql.T.Comparison:
        j += 1
    # Hodnotu tokenu s operatorem si ulozime v uppercase verzi, jinak by napr. "in" bylo malymi pismeny
    operator = t.tokens[j].normalized.upper()
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
    last_token = t.tokens[-1]
    if is_comment(last_token):
        return Attribute(name=name, condition=f"{operator} {value}", comment=last_token.value)
    return Attribute(name=name, condition=f"{operator} {value}")


def get_attribute_conditions(t: sql.Token) -> list:
    """Vraci seznam atributu vc. jejich pozadovanych hodnot. Zadany token muze byt obycejnym porovnanim (Comparison), sekvenci sub-tokenu urcujicich napr. rozmezi hodnot ("rok BETWEEN 2010 AND 2020" apod.), prip. "EXISTS ( ... )"."""

    # TODO: zatim ignoruje logicke spojky mezi podminkami -- je toto ale nutne resit?

    attributes = []
    # Postup se lisi podle toho, zda sqlparse vratil jednoduche srovnani (Comparison), sekvenci tokenu (napr. pro urceni rozmezi hodnot), nebo je toto navic v zavorce (Parenthesis) ci jako soucast [ WHERE | ON ] EXISTS.
    if isinstance(t, sql.Comparison):
        # Token je obycejnym srovnanim, takze staci do kolekce attributes pridat navratovou hodnotu process_comparison(...) (nelze vratit primo tuto navratpovou hodnotu, tzn. objekt typu Attribute, protoze typ navratove hodnoty se pozdeji poziva k rozliseni, jak presne s takovou hodnotou nalozit)
        attributes.append(process_comparison(t))
        return attributes
    if isinstance(t, sql.Identifier):
        # Projdeme t.tokens, pricemz dopredu vime, ze kolekce attributes bude ve vysledku obsahovat jediny atribut
        comment = ""
        i = 0
        token = t.token_first(skip_ws=True, skip_cm=False)
        name = token.value
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
                comment = token.value
            else:
                break
            (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
        value = " ".join(components)
        attributes.append(Attribute(name=name, condition=f"{operator} {value}", comment=comment))
        return attributes
    if isinstance(t, sql.Parenthesis) or isinstance(t, sql.Where):
        # Projdeme t.tokens a postupne rekurzivne zpracujeme kazdy z patricnych sub-tokenu. Zaroven potrebujeme referenci na posledni token v t.tokens, abychom pripadne mohli predat relevantni komentar zpet do hlavni casti kodu. Zohlednit musime i pripadne klicove slovo WHERE.
        last_token = t.tokens[-1]
        comment_before = ""  # Potreba pro pripad, ze by bylo nutne vytvorit mezi-tabulku bez predchoziho vyskytu komentare
        # Prvni token preskocime (jde o oteviraci zavorku, resp. WHERE)
        (i, token) = t.token_next(0, skip_ws=True, skip_cm=False)
        while token != None:
            if is_comment(token):
                comment_before = token.value.strip()
                if token == last_token:
                    # Zde jsme narazili na komentar k mezi-tabulce (napr. "JOIN ... ON ( ... ) komentar"), prip. komentar k nalsedujicimu bloku v SQL kodu. Pridame fiktivni atribut (name == alias == None, condition == "COMMENT", comment != None), ze ktereho pak bude komentar extrahovan.
                    attributes.append(Attribute(name=None, alias=None, condition="COMMENT", comment=token.value))
                    return attributes
                if len(attributes) > 0:
                    # Jde o komentar k poslednimu nalezenemu atributu
                    attributes[-1].comment = token.value.strip()
            elif token.ttype == sql.T.Keyword and token.normalized == "EXISTS":
                # Typicky pripad: "JOIN ... ON ( attr = value AND EXISTS ( ... ) AND ... )" --> postupujeme identicky situaci WHERE EXISTS
                (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
                while token != None:
                    if is_comment(token):
                        # Pripadny komentar si ulozime, jelikoz by se tykal nasledujici mezi-tabulky "EXISTS ( SELECT ... )"
                        comment_before = token.value.strip()
                    elif isinstance(token, sql.Parenthesis):
                        # Nasli jsme zavorku se SELECT, k cemuz je nutne vytvorit patricnou mezi-tabulku
                        exists_table = Table(name_template="exists-select", comment=comment_before)
                        Table.__tables__.append(exists_table)
                        # Nove vytvorenou mezi-tabulku jeste musime svazat s hlavni tabulkou, na kterou tady ale nemame referenci. Pridame proto fiktivni atribut (name == alias == None, condition == "EXISTS_SELECT", comment == ID exists_table), ze ktereho bude patricny udaj v hlavnim kodu extrahovan
                        attributes.append(Attribute(name=None, alias=None, condition="EXISTS_SELECT", comment=str(exists_table.id)))
                        # Zavorku nyni zpracujeme jako standardni statement s tim, ze parametrem predame referenci na vytvorenou mezi-tabulku
                        process_statement(token, exists_table)
                        break
                    (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
            elif isinstance(token, sql.Identifier) or isinstance(token, sql.Function):
                # Nasledujici tokeny v t.tokens budeme prochazet tak dlouho, nez ziskame jednu kompletni podminku. Toto nelze resit rekurzivne opetovnym volanim get_attribute_conditions(...), protoze tokeny musime prochazet na stavajici urovni (token \in t.tokens), nikoliv o uroven nize (token.tokens)
                comment = ""
                name = token.value
                (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
                operator = token.normalized
                (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
                if operator == "IS":
                    value = token.normalized
                    (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
                    if is_comment(token):
                        comment = token.value
                else:
                    components = []
                    while token != None and len(components) < 3:
                        if token.ttype == sql.T.Keyword:
                            components.append(token.normalized)
                        elif is_comment(token):
                            comment = token.value
                        else:
                            # Cokoliv jineho si ulozime (protoze bile znaky preskakujeme pri hledani tokenu a Punctuation apod. tady syntakticky nedava smysl). Ukladame vsak .normalized, cimz dojde k orezani pripadnych internich komentaru.
                            components.append(token.normalized)
                        (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
                    # Jeste musime snizit index (i), abychom nepreskocili aktualni token, ktery muze byt podstatny
                    i -= 1
                    value = " ".join(components)
                attributes.append(Attribute(name=name, condition=f"{operator} {value}", comment=comment))
            elif token.ttype != sql.T.Keyword and token.ttype != sql.T.Punctuation:
                # Jde o obycejny atribut (prip. jejich vycet)
                attributes.extend(get_attribute_conditions(token))
            # Nakonec musime prejit na dalsi token
            (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
        return attributes


def process_with_element(t, comment_before="") -> str:
    """Kompletne zpracuje kod bloku ve WITH (vc. vytvoreni odpovidajici tabulky) a vrati komentar, ktery byl na konci t.tokens (protoze ten se dost mozna vztahuje ke kodu, ktery nasleduje po aktualne zpracovavanem bloku)."""
    # Struktura tokenu nasledujicich po klicovem slove WITH: Identifier [ [ whitespace(s) ] Punctuation [ whitespace(s) ] Identifier [ ... ] ]
    # --> Zde zpracovavame pouze tokeny typu Identifier. Pokud narazime napr. na bily znak nebo Punctuation, jednoduse vratime comment_before tak, jak jsme ho dostali.
    comment_after = comment_before
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
        last_token = t.tokens[-1]
        if is_comment(last_token):
            comment_after = last_token.value.strip()
        else:
            comment_after = ""
        table = Table(name=name, comment=comment_before, source_sql=t.value)
        Table.__tables__.append(table)
        if len(aliases) > 0:
            # Zname uz aliasy atributu (byly v zavorce za nazvem tabulky), ale nic vic k atributum tabulky nevime. Pouze tedy nastavime parametr, na zaklade ktereho pak v hlavni casti kodu (process_statement(...)) budou k atributum doplneny zbyle udaje. Jmena atributu budou pro poradek (at nejsou None) docasne "TBD".
            known_attribute_aliases = True
            for a in aliases:
                table.attributes.append(Attribute(name="TBD", alias=a))
        else:
            known_attribute_aliases = False
        # Nakonec doresime zavorku, odkaz na jiz vytvorenou tabulku predame stejne jako parametr ohledne (ne)znalosti aliasu atributu
        process_statement(t.tokens[i], table, known_attribute_aliases)
    return comment_after


def process_identifier_or_function(t) -> list:
    """Zpracuje token typu Identifier nebo Function a vrati odpovidajici atribut. Je-li pro popsani atributu potreba mezi-tabulka (napr. pokud je misto obycejneho atributu "( SELECT ... )" nebo "( CASE ... )"), vrati krome odpovidajiciho atributu i fiktivni atribut s udajem pro svazani nadrazene tabulky s nove vytvorenou mezi-tabulkou (name == alias == condition == None, comment == ID mezi-tabulky)."""
    attributes = []
    # Jmeno a pripadny alias zjistime pomoci get_name_alias_comment(...)
    name, alias, comment = get_name_alias_comment(t)
    # Do pomocne promenne si ulozime kod v tokenu (bude potreba nize)
    if len(name) > 1:
        leading_portion = name[1:].strip().lower()
    else:
        leading_portion = name
    # Resime kompletni token (byt treba nestandarni -- v zavorce), nebo jde o zbytek tokenu s WITHIN GROUP?
    if (isinstance(t.tokens[0], sql.Parenthesis)
            and not leading_portion.startswith("order")):
        # Namisto bezneho atributu pracujeme se zavorkou, ve ktere typicky byva dalsi SELECT, prip. CASE. S ohledem na moznou delku SELECTu vezmeme jako nazev atributu pouze nazev odpovidajici mezi-tabulky, nastavime pripadny alias (pokud je za zavorkou uveden), obsah zavorky zpracujeme jako separatni statement (podobne jako napr. JOIN). Nakonec nastavime zavislosti tabulek. U CASE namisto vytvoreni mezi-tabulky atd. jednoduse pouzijeme cely kod jako nazev atributu (podobne jako v pripade funkce) a nastavime pripadny alias a komentar, coz uz vsechno mame z get_name_alias_comment(...) volaneho vyse.
        # Nejprve tedy musime zjistit, co konkretne vlastne nyni je v aktualnim tokenu. Toto udelame naprosto "tupe" prostym porovnanim zacatku leading_portion se "select" (melo by snad stacit).
        if leading_portion.startswith("select"):
            # Vytvorime mezi-tabulku, u ktere jako komentar nastavime ten vyse zjisteny, a aktualizujeme jmeno atributu podle jmena tabulky
            table = Table(name_template="select", comment=comment)
            Table.__tables__.append(table)
            # Sub-token se SELECT je hned jako prvni, neni potreba hledat ho iterovanim pres token.tokens
            process_statement(t.tokens[0], table)
            name = f"<{table.name}>"
            # Nakonec je nutne zaridit nastaveni zavislosti nadrazene tabulky. Na tu ale zde nemame k dispozici odkaz. ID nove mezi-tabulky tedy predame jako fiktivni atribut (name == alias == None, condition == "SELECT", comment == ID) a zavislost (prip. zavislosti, nebot jich muze byt vice) pak doresime v hlavnim kodu.
            attributes.append(Attribute(name=None, alias=None, condition="SELECT", comment=str(table.id)))
    # Nakonec do kolekce pridame samotny atribut, at uz je obycejny nebo nekompletni (toto se doresi v process_token(...))
    attributes.append(Attribute(name=name, alias=alias, comment=comment))
    return attributes


def process_token(t, is_within=None, comment_before="") -> Any:
    """Zpracuje zadany token; typ vraceneho objektu zavisi na tom, jakeho typu token je a v jakem kontextu se nachazi (napr. SELECT <token> ... vrati odkaz na vytvorenou tabulku apod.)"""
    if is_within != None and "select" in is_within:
        # Token je v kontextu lib. mutace SELECT (std., UNION SELECT, ...). Pokud je token typu Parenthesis, je potreba vytvorit odpovidajici (mezi-)tabulku a zavorku pak zpracovat jako samostatny SQL statement. Do process_statement(...) pritom musime predat odkaz na novou tabulku, aby bylo mozne spravne priradit nalezene atributy atd. Krome toho muze token reprezentovat i "( SELECT ...) AS ..." nebo "( CASE ... ) AS ..." ve vyctu atributu.
        if isinstance(t, sql.Parenthesis):
            # Zde resime UNION SELECT nebo "SELECT ... FROM ( SELECT ... )"; nemuze jit o "( SELECT ...) AS ..." nebo "( CASE ... ) AS ..." ve vyctu atributu, protoze tam musi byt alias (a takovy token tedy je typu Identifier[List])
            table = Table(name_template=is_within, comment=comment_before)
            Table.__tables__.append(table)
            process_statement(t, table)
            return table
        # POZOR: sqlparse neumi WITHIN GROUP(...) (napr. "SELECT LISTAGG(pt.typ_program,', ') WITHIN GROUP(ORDER BY pt.typ_program) AS programy FROM ...") --> BUG report ( https://github.com/andialbrecht/sqlparse/issues/700 ). Podobne je nekdy vracena funkce COUNT (a nejspis i jine funkce) -- nazev fce je vracen jako klicove slovo na konci Identifier (za carkou; resp. posledniho Identifieru v IdentifierList) a zavorka s parametry pak jako zacatek naledujiciho tokenu.
        # Bugy vyse prozatim obejdeme tak, ze pri zpracovavani vzdy overime posledni subtoken (Identifier WITHIN (vraceno jako Identifier), resp. Keyword s nazvem funkce -- pokud ano, je temer jiste, ze jde o zminenou situaci a posledni nalezeny atribut pak bude nekompletni (--> nastavime u nej condition na "SPLIT_ATTRIBUTE", podle cehoz pak v hlavnim kodu pozname, ze tento je nekompletni). Takovy nekompletni atribut pritom muze vzdy byt uveden pouze jako posledni ve vracenem seznamu atributu.
        split_attr_link = None
        if t.value.lower().endswith("within"):
            # Musi byt s mezerami na zacatku/konci, aby naopak funkce (COUNT apod.) mohly byt bez mezer mezi nazvem a zavorkou
            split_attr_link = " WITHIN GROUP "
        elif isinstance(t, sql.IdentifierList) and t.tokens[-1].ttype == sql.T.Keyword:
            split_attr_link = ""
        # Je-li token typu Identifier, IdentifierList, Function, prip. Wildcard, jde o obycejny atribut ci seznam atributu. Metoda pak podle toho vrati seznam s jednim ci vicero atributy.
        attributes = []
        if isinstance(t, sql.Identifier) or isinstance(t, sql.Function):
            attr = process_identifier_or_function(t)
            attributes.extend(attr)
        elif isinstance(t, sql.IdentifierList):
            # Zde postupujeme analogicky pripadu vyse, jen s tim rozdilem, ze musime projit vsechny tokeny v t.tokens
            for token in t.tokens:
                if isinstance(token, sql.Identifier) or isinstance(token, sql.Function):
                    attributes.extend(process_identifier_or_function(token))
                elif token.ttype in sql.T.Literal or token.ttype == sql.T.Keyword:
                    # Nasli jsme literal (typicky v situaci, kdy je ve WITH definovana pomocna tabulka s konkretnimi -- v SQL kodu zadanymi -- hodnotami)
                    attributes.append(Attribute(name=token.normalized))
        elif t.ttype == sql.T.Wildcard:
            # Typicky "SELECT * FROM ..."
            attributes.append(Attribute(name="*"))
        elif t.ttype in sql.T.Literal:
            # Nasli jsme literal (typicky v situaci, kdy je ve WITH definovana pomocna tabulka s konkretnimi -- v SQL kodu zadanymi -- hodnotami)
            attributes.append(Attribute(name=t.value))
        # Nakonec jeste condition na "SPLIT_ATTRIBUTE" a comment na patricny spojovaci retezec, pokud je posledni atribut nekompletni
        if split_attr_link != None:
            attributes[-1].condition = "SPLIT_ATTRIBUTE"
            attributes[-1].comment = split_attr_link
        return attributes
    if is_within == "from" or is_within == "join":
        # Token je v kontextu FROM ("SELECT ... FROM <token>"), prip. JOIN (napr. "SELECT ... FROM ... INNER JOIN <token>"). V obou pripadech muze byt token jak typu Parenthesis ("SELECT ... FROM ( SELECT ... )", "... JOIN ( SELECT ... )"), tak muze jit o prosty nazev zdrojove tabulky + pripadny alias a komentar.
        # Zde navic mohou nastat dva pripady: bud je za zavorkou alias a/nebo komentar (--> jako statement zpracujeme t.tokens[0]), nebo je v SQL kodu pouze zavorka (--> jako statement zpracujeme cely token).
        if isinstance(t, sql.Parenthesis):
            # Pripadny komentar by byl az za zavorkou, tzn. comment_before muzeme ignorovat
            table = Table(name_template="select")
            Table.__tables__.append(table)
            process_statement(t, table)
            return table
        if isinstance(t.tokens[0], sql.Parenthesis):
            # Struktura t.tokens: parenthesis-SELECT [ whitespace(s) [AS whitespace(s) ] alias [ whitespace(s) komentar ] ]
            # V zavorce je vzdy SELECT, takze je potreba vytvorit odpovidajici (mezi-)tabulku (sablona == "select")
            table = Table(name_template="select")
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
                table.add_alias("".join(components))
            # Dale zkontrolujeme posledni token, zda je komentarem. Pokud je a u tabulky zaroven neni nastaveny zadny komentar z drivejska (prvni komentar byva obvykle podrobnejsi nez pripadny dalsi k teze tabulce), tento komentar ulozime.
            last_token = t.tokens[-1]
            if (is_comment(last_token)
                    and (table.comment == None or len(table.comment) == 0)):
                table.comment = last_token.value.strip()
            # Uplne nakonec pak zpracujeme prvni subtoken (t.tokens[0]) jako samostatny statement a odkaz na vytvorenou tabulku predame parametrem
            process_statement(t.tokens[0], table)
            return table
        # V pripade, ze token (prip. prvni subtoken) neni typu Parenthesis, jde o prosty nazev zdrojove tabulky + pripadny alias a komentar. Tyto ziskame jednoduse zavolanim get_name_alias_comment(...).
        return get_name_alias_comment(t)
    if is_within == "with":
        # Token je v kontextu WITH -- Identifier ("WITH <token: name AS ( SELECT ... )>"), IdentifierList ("WITH <token: name_1 AS ( SELECT ... ), name_2 AS ( SELECT ... ), ...>")
        if isinstance(t, sql.Identifier):
            # WITH obsahuje pouze jeden blok (docasnou tabulku) --> zpracujeme metodou process_with_element(...); zaroven musime vratit pripadny komentar, ktery je poslednim tokenem v t.tokens, byt se dost mozna vztahuje az k nasledujicimu tokenu
            return process_with_element(t, comment_before)
        if isinstance(t, sql.IdentifierList):
            # Jednotlive tokeny v zpracujeme analogicky pripadu vyse. Pritom je nutne postupne predavat nalezene komentare a nakonec vratit posledni vystup metody process_with_element(...).
            for token in t.tokens:
                comment_before = process_with_element(token, comment_before)
            return comment_before
    if is_within == "on":

        # TODO: bug s COUNT apod. mozna muze byt relevantni i zde? --> OVERIT

        # Token je v kontextu ON ("SELECT ... JOIN ... ON <token>"). Zde tedy jde o atributy vc. hodnot, ktere u nich pozadujeme
        return get_attribute_conditions(t)


def process_statement(s, table=None, known_attribute_aliases=False) -> None:
    """Zpracuje cely SQL statement vc. vytvoreni patricnych tabulek"""
    # CTE ... Common Table Expression (WITH, ...)
    # DDL ... Data Definition Language (...)
    # DML ... Data Manipulation Language (SELECT, ...)
    # Tokeny budeme prochazet iteratorem a rovnou budeme preskakovat bile znaky (komentare vsak ne)
    i = 0
    t = s.token_first(skip_ws=True, skip_cm=False)
    # Flag pro predavani informaci o kontextu toho ktereho tokenu (v ruznych kontextech je zpravidla potreba mirne odlisny zpusob zpracovani)
    is_within = None
    comment_before = ""
    # Pocitadlo radku od posledniho komentare (nekdy nas zajima komentar pred aktualnim tokenem). Pocatecni hodnota je libovolna takova, aby se v cyklu na zacatku NEresetoval comment_before, pokud by SQL dotaz nezacinal komentarem.
    token_counter = 10
    # Zdrojovy kod:
    #   * WITH: lze primo pomoci t.value
    #   * JOIN: nutno skladat po castech (oddelene tokeny)
    #   * SELECT: u "( SELECT ... )" sice lze pouzit t.parent.value, ale toto u top-level SELECT (bez uvedeni v zavorkach) ulozi vzdy kompletne cely (!) SQL dotaz, coz neni zadouci. I zde tedy jsou zdrojove kody skladany po castech.
    sql_components = []
    join_components = []
    # union_* jsou potreba v pripade, ze sjednocovani je provadeno bez prikazu "SELECT ..." v zavorce (tzn. "SELECT ... UNION SELECT ..."), jelikoz pak je patricny SQL kod vracen jako prosta sekvence tokenu). Pokud je nektery SELECT v zavorkach, zpracovava se jako samostatny statement.
    union_components = []
    union_table = None
    # Nekompletni atribut vznikly v dusledku WITHIN GROUP, OVER apod. (viz mj. bugy zminene v process_token(...)); pokud neni None, je potreba ho sloucit s nekompletnim prvnim atributem vracenym v "dalsim kole" zpracovavani atributu
    split_attribute = None
    while t != None:
        # Jsme-li dva tokeny od posleniho komentare, muzeme resetovat comment_before (reset po jednom tokenu nelze, jelikoz jednim z nich muze byt carka mezi SQL bloky a komentar k takovemu bloku pak je typicky na radku pred touto carkou)
        token_counter += 1
        if token_counter == 2:
            comment_before = ""
        if is_comment(t):
            # Pri nalezeni komentare si tento ulozime a resetujeme token_counter
            comment_before = t.value.strip()
            token_counter = 0
        elif t.ttype == sql.T.Keyword:
            # Narazili jsme na klicove slovo, coz ve vetsine pripadu (viz dale) vyzaduje nastaveni is_within na patricny kontext
            if t.normalized == "FROM":
                is_within = "from"
            elif "JOIN" in t.normalized:
                is_within = "join"
                # Zde musime krome nastaveni kontextu navic resetovat join_components...
                join_components = []
                # ... a pokud jsme doted resili UNION SELECT (tzn. pokud union_table != None), je take nutne k union_table pridat zdrojovy SQL kod a resetovat referenci na tabulku (UNION je totiz timto doreseny)
                if union_table != None:
                    union_table.source_sql = "\n".join(union_components).strip()
                    union_table = None
            elif t.normalized == "ON":
                is_within = "on"
            elif "UNION" in t.normalized:
                is_within = "union-select"
            elif t.normalized == "OVER":  # V zasade by slo resit pomoci over_ahead, ale nebylo by nijak znatelne rychlejsi...
                # Tato cast je nutna pro rucni obejiti chyby v sqlparse (BUG https://github.com/andialbrecht/sqlparse/issues/701 )
                # Klicove slovo OVER a nasledna zavorka s pripadnym PARTITION BY apod. jsou vraceny jako dva tokeny oddelene od predchoziho tokenu s funkci. Pripadny alias a komentar jsou az soucasti tokenu se zavorkou. Prvni token s OVER tedy pridame do sql_components a nasledne z druheho tokenu zjistime pripadny alias a komentar.
                split_attribute = table.attributes[-1]
                split_attribute.comment = " OVER "
                # Zaroven musime snizit hodnotu token_counter o 2, jelikoz umelym rozdelenim bloku atributu na vicero tokenu kvuli OVER nacteme o 2 tokeny vice
                token_counter -= 2
            elif (t.normalized == "ORDER BY"
                    or t.normalized == "GROUP BY"):
                # V tomto pripade se zda, ze parametry (jeden, prip. vice) jsou vzdy vraceny jako jeden token. Akt. token tedy ulozime do kolekci sql_components, join_components a union_components a nacteme dalsi token (cimz nasledujici token de facto preskocime).
                sql_components.append(t.value)
                join_components.append(t.value)
                union_components.append(t.value)
                (i, t) = s.token_next(i, skip_ws=True, skip_cm=False)
            elif t.normalized == "CONNECT":
                # Zde nelze obecne rici, jakym zpusobem budou tokeny vraceny (nepovinna klicova slova, Identifier vs. Builtin + Comparison + Integer vs. ...). Nasledujici tokeny tedy musime prochazet a ukladat do kolekci (sql_components atd.) tak dlouho, nez najdeme Comparison. POZOR: "...ttype != sql.T.Comparison" NENI TOTEZ JAKO "not isinstance(..., sql.Comparison)"!
                while t != None and not (t.ttype == sql.T.Comparison or isinstance(t, sql.Comparison)):
                    sql_components.append(t.value)
                    join_components.append(t.value)
                    union_components.append(t.value)
                    (i, t) = s.token_next(i, skip_ws=True, skip_cm=False)
                # Comparison si ulozime do kolekci
                sql_components.append(t.value)
                join_components.append(t.value)
                union_components.append(t.value)
                # Ted jeste overime, zda je nasl. token Literal, prip. zavorka. Pokud neni, preskocime na zacatek cyklu, jinak pokracujeme na konec cyklu (ulozeni hodnoty + nacteni noveho tokenu).
                (i, t) = s.token_next(i, skip_ws=True, skip_cm=False)
                if not (t.ttype in sql.T.Literal or isinstance(t, sql.Parenthesis)):
                    continue
            else:

                # DEBUG
                if (t.normalized != "DISTINCT"
                        and t.normalized != "GROUP"
                        and not (is_within == "on" and t.normalized == "AND")):
                    print(f"\n>>> POTENCIALNE PROBLEMATICKE KLICOVE SLOVO: {t.normalized}\n")

            # Kazde jine vyse neuvedene klicove slovo (u kterych nepredpokladame vyskyt parametru) proste na konci tohoto cyklu ulozime a nacteme dalsi token
        elif t.ttype == sql.T.CTE and t.normalized == "WITH":
            is_within = "with"
        elif t.ttype == sql.T.DML and t.normalized == "SELECT":
            if is_within == "union-select":
                # Spojovane SELECTy mohou byt vc. WHERE apod. a slouceni vsech atributu takovych SELECTu pod nadrazenou tabulku by nemuselo davat smysl. Pokud tedy po UNION [ALL] nasleduje SELECT (bez uvedeni v zavorkach), budou odpovidajici tokeny vraceny sqlparse postupne a tudiz musime uz zde vytvorit patricnou mezi-tabulku. Jinak receno, k situaci je nutne pristupovat podobně jako u JOIN. Je-li SELECT v zavorkach, zpracuje se dale jako samostatny statement.
                union_table = Table(name_template="union-select", comment=comment_before)
                Table.__tables__.append(union_table)
                union_components = []
            else:
                is_within = "select"
                # Pokud jde o SELECT na nejvyssi urovni dotazu, neexistuje pro nej zatim zadna tabulka. Tuto tedy vytvorime, aby k ni pak bylo mozne doplnit atributy atd.
                if table == None:
                    table = Table(name_template="select", comment=comment_before)
                    Table.__tables__.append(table)
                sql_components = []
        elif isinstance(t, sql.Where):
            attributes = get_attribute_conditions(t)
            # Vznikly pri zpracovavani podminek nejake mezi-tabulky pro "EXISTS ..."? Pokud ano, stavajici tabulku musime nyni navazat na vsechny takove tabulky pomoci vracenych fiktivnich atributu (name == alias == None, condition == "EXISTS_SELECT", comment == ID exists_table).
            j = 0
            while j < len(attributes):
                attribute = attributes[j]
                if (attribute.name == None
                        and attribute.alias == None
                        and attribute.condition == "EXISTS_SELECT"
                        and attribute.comment != None):
                    table.link_to_table_id(int(attribute.comment))
                    attributes.pop(j)
                else:
                    j += 1
            # Pokud jsme pri nacitani atributu nasli jako posledni sub-token komentar, jde temer jiste o komentar k nasledujicimu bloku SQL kodu. Fiktivni atribut s nesmyslnymi parametry (name == alias == None, condition == "COMMENT", comment != None) nyni komentar ziskame zpet a aktualizujeme pomoci nej comment_before.
            if len(attributes) > 0:
                last_attribute = attributes[-1]
                if (last_attribute.name == None
                        and last_attribute.alias == None
                        and last_attribute.condition == "COMMENT"
                        and last_attribute.comment != None):
                    comment_before = last_attribute.comment
                    attributes.pop()
            # Nyni muzeme aktualizovat atributy u patricne tabulky (union_table, resp. table -- dle situace)
            if union_table != None:
                union_table.update_attributes(attributes)
            else:
                table.update_attributes(attributes)
        elif not t.ttype == sql.T.Punctuation:
            # Jakykoliv jiny token (tedy pokud nejde o Punctuation) zpracujeme "obecnou" metodou process_token(...) s tim, ze parametrem predame informaci o kontextu (is_within) a pripadnem komentari pred tokenem (comment_before).
            # Timto vyresime napr. i tokeny typu "select ... from ... PIVOT (...)" (typ: Function), jleikoz v miste uziti PIVOT uz je is_within == None, tzn. process_token(...) vrati None.
            obj = process_token(t, is_within, comment_before)
            # Navratova hodnota process_token(...) muze byt ruznych typu v zavislosti na kontextu apod. Na zaklade toho se nyni rozhodneme, jakym konkretnim zpusobem je potreba s ni nalozit.
            if obj != None:
                if isinstance(obj, list) and isinstance(obj[0], Attribute):
                    # Ziskali jsme seznam atributu
                    if is_within == "on":
                        # Zkontrolujeme, zda mezi podminkami nebylo "EXISTS ( SELECT ... )", a pripadne aktualizujeme zavilosti u join_table
                        j = 0
                        while j < len(obj):
                            attribute = obj[j]
                            if (attribute.name == None
                                    and attribute.alias == None
                                    and attribute.condition == "EXISTS_SELECT"
                                    and attribute.comment != None):
                                join_table.link_to_table_id(int(attribute.comment))
                                obj.pop(j)
                            else:
                                j += 1
                        # Pokud jsme pri nacitani atributu v "JOIN ... ON ..."" nasli jako posledni sub-token komentar, jde o komentar k mezi-tabulce reprezentujici JOIN. Do seznamu atributu byl v takovem pripade jako posledni pridat fiktivni atribut s nesmyslnymi parametry (name == alias == None, condition == "COMMENT", comment != None), ze ktereho nyni komentar ziskame zpet a priradime ho k dane tabulce.
                        if len(obj) > 0:
                            last_attribute = obj[-1]
                            if (last_attribute.name == None
                                    and last_attribute.alias == None
                                    and last_attribute.condition == "COMMENT"
                                    and last_attribute.comment != None):
                                join_table.comment = last_attribute.comment
                                obj.pop()
                        # Vraceny objekt (nyni uz bez pripadneho fiktivniho atributu s komentarem) muzeme pouzit k aktualizaci atributu i mezitabulky reprezentujici JOIN
                        join_table.update_attributes(obj)

                        # TODO: mozna updatovat attributy v OBOU tabulkach z JOIN? (pozor: nelze podle LHS/RHS -- bylo by potreba delat podle referenci na tabulky v nazvech atributu)
                        
                        # Hodnotu tokenu si pridame to kolekce s komponentami zdrojoveho SQL kodu
                        join_components.append(t.value)
                        # Jelikoz nyni mame cely JOIN zpracovany, lze k mezi-tabulce priradit i ji odpovidajici SQL kod
                        join_table.source_sql = "\n".join(join_components).strip()
                    elif is_within == "union-select":
                        # Resime-li UNION SELECT, staci pridat nalezene autributy k mezi-tabulce reprezentujici danou cast kodu
                        union_table.attributes.extend(obj)
                    elif is_within == "select":
                        # Projdeme vraceny seznam, ktery muze obsahovat fiktivni atributy s ID tabulek (name == alias == None, condition == "SELECT", comment == ID), na nichz zavisi aktualne resena tabulka (typicky scenar: namisto obycejneho atributu je v SELECT uveden dalsi SELECT)
                        j = 0
                        while j < len(obj):
                            attribute = obj[j]
                            if (attribute.name == None
                                    and attribute.alias == None
                                    and attribute.condition == "SELECT"
                                    and attribute.comment != None):
                                table.link_to_table_id(int(attribute.comment))
                                obj.pop(j)
                            else:
                                j += 1
                        # Dale musime zkontrolovat, jestli nemame ze zpracovavani minuleho tokenu nekomplentni atribut (BUG: WITHIN GROUP apod.). Pokud ne, zkontrolujeme posledni nyni vraceny atribut, zda nahodou neni takovym objektem. Jestlize naopak nekompletni atribut mame, sloucime ho s prvnim nyni vracenym atributem (ktery nasledne odebereme z obj) a takto vznikly kompletni atribut pridame k tabulce. Zde nelze rovnou resetovat split_attribute, jelikoz i zde muze byt posledni atribut opet nekompletni...
                        if split_attribute == None:
                            if obj[-1].condition == "SPLIT_ATTRIBUTE":
                                split_attribute = obj.pop()
                                # Zaroven musime snizit hodnotu token_counter o 1, jelikoz umelym rozdelenim bloku atributu na vicero tokenu nacteme o 1 token vice
                                token_counter = max(0, token_counter - 1)
                        else:
                            attr_remainder = obj.pop(0)
                            # Pokud napr. mezi "GROUP" a nasledujici zavorkou neni mezera, je pokracovani tokenu vc. klicoveho slova "GROUP". Zkontrolujeme tedy, zda jmeno attr_remainder zacina na "(" -- pokud ne a spojovaci reteze (split_attribute.comment) zaroven obsahuje vice nez jedno slovo, to posledni z nej odstranime.
                            attr_link = split_attribute.comment.split()
                            if not attr_remainder.name.startswith("(") and len(attr_link) > 1:
                                attr_link.pop()
                                split_attribute.comment = " " + " ".join(attr_link) + " "
                            split_attribute.name = f"{split_attribute.name}{split_attribute.comment}{attr_remainder.name}"
                            split_attribute.alias = attr_remainder.alias
                            split_attribute.condition = attr_remainder.condition
                            split_attribute.comment = attr_remainder.comment
                            table.attributes.append(split_attribute)
                            if len(obj) > 0 and obj[-1].condition == "SPLIT_ATTRIBUTE":
                                split_attribute = obj.pop()
                                # Zaroven musime snizit hodnotu token_counter o 1, jelikoz umelym rozdelenim bloku atributu na vicero tokenu nacteme o 1 token vice
                                token_counter -= 1
                            else:
                                split_attribute = None
                        # Nakonec k tabulce pridame atributy zbyle v obj
                        table.attributes.extend(obj)
                    elif known_attribute_aliases:
                        # Zde resime blok ve WITH, u ktereho byly za nazvem docasne tabulky uvedeny aliasy (alespon nekterych) atributu. Nejprve tedy zkontrolujeme, zda pocet atributu v kompletnim seznamu >= poctu aliasu uvedenych drive v zavorce za jmenem tabulky. pokud tomu tak neni, je s SQL kodem neco spatne.
                        if len(obj) < len(table.attributes):
                            raise(f"Počet aliasů atributů v tabulce {table.name} je větší než počet hodnot vracených příkazem SELECT")
                        # Vime, ze aliasy atributu tabulky ve WITH musely byt uvedeny ve stejnem poradi jako atributy nyni zjistene z prikazu SELECT. Atributy u tabulky proto na zaklade jejich poradi aktualizujeme podle objektu vraceneho vyse metodou process_token(...).
                        for j in range(len(table.attributes)):
                            table.attributes[j].name = obj[j].name
                            table.attributes[j].condition = obj[j].condition  # TODO: mozna neni potreba? (attrib conditions jsou nastavovany pouze v pripade JOIN)
                            table.attributes[j].comment = obj[j].comment
                        # nakonec pridame pripadne dalsi atributy, ktere byly zjisteny nad ramec aliasu uvedenych za nazvem tabulky
                        for j in range(len(table.attributes), len(obj)):
                            table.attributes.append(obj[j])
                    else:
                        # Ve zbylych situacich staci pridat nalezene atributy k aktualni tabulce (ktera uz u korektniho SQL kodu nyni nemuze byt None)
                        table.attributes.extend(obj)
                elif isinstance(obj, tuple) and isinstance(obj[0], str):
                    # Metoda process_token(...) vratila ntici, v niz je prvni prvek retezcem. Jinak receno, ziskali jsme nazev tabulky spolu s pripadnym aliasem a komentarem. Nejprve tedy zkusime najit zdrojovou tabulku, odkud se berou data, a pridame k ni alias.
                    src_table = Table.get_table_by_name(obj[0])
                    if src_table == None:
                        # Zdrojova tabulka zatim neexistuje (typicky v situaci, kdy resime "SELECT ... FROM dosud_nezminena_tabulka") --> vytvorime ji
                        src_table = Table(name=obj[0], alias=obj[1], comment=obj[2])
                        Table.__tables__.append(src_table)
                    else:
                        # O zdrojove tabulce uz vime, takze k ni jen pridame alias.
                        src_table.add_alias(obj[1])
                        # Komentar pridame jen v pripade, ze tento zatim neni nastaveny (prvotni komentar zpravidla byva detailnejsi a nedava smysl ho prepsat necim dost mozna kratsim/strucnejsim)
                        if src_table.comment == None or len(src_table.comment) == 0:
                            src_table.comment = obj[2]
                    if is_within == "join":
                        # Pokud resime JOIN, vytvorime patricnou mezi-tabulku (zatim neexistuje!), ke ktere budou nasledne pridany atributy s podminkami dle ON
                        join_table = Table(name_template="join")
                        Table.__tables__.append(join_table)
                        # Navic je nutne nastavit zavislosti tabulek: table --> join_table --> src_table
                        table.link_to_table_id(join_table.id)
                        join_table.link_to_table_id(src_table.id)
                    elif union_table != None:
                        # Pokud resime UNION, mezi-tabulka uz existuje, takze pouze nastavime zavislosti (table --> union_table --> src_table)
                        table.link_to_table_id(union_table.id)
                        union_table.link_to_table_id(src_table.id)
                    else:
                        # V "obecnem" pripade ("SELECT ... FROM src_table") proste jen k aktualni tabulce reprezentujici SELECT pridame zavislost na zdrojove tabulce
                        table.link_to_table_id(src_table.id)
                elif isinstance(obj, Table):
                    # Metoda process_token(...) vratila objekt typu Table. Toto muze nastat ve dvou pripadech: bud resime JOIN (k cemuz musime vytvorit mezi-tabulku a nastavit odpovidajici zavislosti), nebo jde o situaci "SELECT ... FROM ( SELECT ... )" (kde uz mezi-tabulka byla vytvorena -- jde o tu vracenou -- a pouze nastavime zavislost aktualni tabulky na mezi-tabulce).
                    if is_within == "join":
                        join_table = Table(name_template="join")
                        Table.__tables__.append(join_table)
                        # Zavislosti: table --> join_table --> src_table
                        table.link_to_table_id(join_table.id)
                        join_table.link_to_table_id(obj.id)
                    else:
                        table.link_to_table_id(obj.id)
                elif is_within == "with":  # and isinstance(obj, str):  # Neni potreba, metoda v kontextu WITH vraci vyhradne retezec (nanejvys prazdny)
                    # Resime blok WITH, kde navratovou hodnotou je pripadny komentar (byva vracen vzdy jako posledni sub-token, i kdyz se muze tykat az nasledujiciho tokenu). Ten si tedy ulozime a resetujeme token_counter.
                    comment_before = obj
                    token_counter = 0
            # Nyni musime jako prvni vec zkontrolovat, jestli nasledujici token neni s hodnotou "DATA", coz sqlparse oznaci za Keyword (pravdepodobne BUG). Pokud tomu tak je, jde o alias k predchozimu nazvu atributu nebo tabulky, ktery adekvatne priradime, ulozime stavajici token do kolekci, opravime hodnotu indexu (i) a promenne drzici token (t) a nacteme novy next_token.
            # Dale, pokud neexistuje zadny nekompletni atribut (dalsi BUG: WITHIN GROUP, OVER, ...), muzeme resetovat kontext. K tomu ale musime taktez zkontrolovat nasledujici token. Podobne overime, jestli nenasleduje AND, coz by znacilo napr. pokracovani podminky v JOIN ... ON podm1 AND podm2 AND ...
            over_ahead = False
            and_ahead = False
            (j, next_token) = s.token_next(i, skip_ws=True, skip_cm=False)
            if next_token != None and next_token.ttype == sql.T.Keyword:
                if next_token.value.upper() == "DATA":
                    # Priradime alias podle aktualne reseneho kontextu (pri jinak syntakticky spravnem SQL kodu musi nyni byt obj != None, tzn. neni potreba toto kontrolovat)
                    if isinstance(obj, list) and isinstance(obj[0], Attribute):
                        if is_within == "on":
                            join_table.attributes[-1].alias = next_token.value
                        elif is_within == "union-select":
                            union_table.attributes[-1].alias = next_token.value
                        elif is_within == "select":
                            table.attributes[-1].alias = next_token.value
                    elif isinstance(obj, tuple) and isinstance(obj[0], str):
                        src_table.add_alias(next_token.value)
                    # Ulozime hodnotu akt. tokenu
                    sql_components.append(t.value)
                    join_components.append(t.value)
                    union_components.append(t.value)
                    # Aktualizujeme index a akt. token
                    i = j
                    t = next_token
                    # Nacteme novy next_token
                    (j, next_token) = s.token_next(i, skip_ws=True, skip_cm=False)
                # Zde musime znovu zkontrolovat, zda i pripadny novy next_token neni None atd.
                if next_token != None and next_token.ttype == sql.T.Keyword:
                    over_ahead = next_token.normalized == "OVER"
                    and_ahead = next_token.normalized == "AND"
            # # Kod nize staci v pripade, ze se neresi Keyword/alias "DATA"
            # (j, next_token) = s.token_next(i, skip_ws=True, skip_cm=False)
            # over_ahead = False
            # and_ahead = False
            # if next_token != None and next_token.ttype == sql.T.Keyword:
            #     over_ahead = next_token.normalized == "OVER"
            #     and_ahead = next_token.normalized == "AND"
            if split_attribute == None and not over_ahead and not and_ahead:
                is_within = None
        # Nakonec si ulozime kod otkenu do kolekci sql_components, join_components a union_components (je nutne aktualizovat vsechny!) a nacteme dalsi token
        sql_components.append(t.value)
        join_components.append(t.value)
        union_components.append(t.value)
        (i, t) = s.token_next(i, skip_ws=True, skip_cm=False)
    # Obsah sql_components se resetuje pri nalezeni SELECT, resp. JOIN. Pokud je SELECT v zavorkach ("SELECT ... FROM ( SELECT ... )"), obsahuje kolekce na konci jednu uzaviraci zavorku navic, kterou je pred ulozenim SQL kodu nutne odstranit.
    if len(sql_components) > 0 and sql_components[0].lower() == "select":
        if sql_components[-1] == ")":
            sql_components.pop()
        table.source_sql = "\n".join(sql_components).strip()


if __name__ == "__main__":
    # Z parametru nacteme nazev souboru se SQL kodem a pozadovane kodovani (prvni parametr obsahuje nazev skriptu)
    if len(sys.argv) > 2:
        source_sql = str(sys.argv[1])
        encoding = str(sys.argv[2])
    else:
        # # Pokud bylo zadano malo parametru, zobrazime napovedu a ukoncime provadeni skriptu
        # print("\nSyntaxe:\n\n  sql2xml SOUBOR KODOVANI\n\nkde:\n  SOUBOR    cesta k souboru s SQL dotazem\n  KODOVANI  kódování, které má být použito při čtení souboru výše\n            (ansi, utf-8 apod.)\n")
        # os._exit(1)  # sys.exit(1) vyvola dalsi vyjimku (SystemExit)!

        # DEBUG
        # source_sql = "./test-files/EI_znamky_2F_a_3F__utf8.sql"
        # source_sql = "./test-files/Plany_prerekvizity_kontrola__utf8.sql"
        # source_sql = "./test-files/Predmety_aktualni_historie__utf8.sql"
        # source_sql = "./test-files/Predmety_aktualni_historie_MOD__utf8.sql"
        # source_sql = "./test-files/sql_parse_pokus__utf8.sql"
        # encoding = "utf-8"
        source_sql = "./test-files/PHD_studenti_SDZ_SZZ_predmety_publikace__utf8-sig.sql"
        # source_sql = "./test-files/PHD_studenti_SDZ_SZZ_predmety_publikace_MOD__utf8-sig.sql"
        # source_sql = "./test-files/Predmety_literatura_pouziti_v_planech_Apollo__utf8-sig.sql"
        # source_sql = "./test-files/Profese_Pridelene_AD_vymazat_orgunitu__utf8-sig.sql"
        # source_sql = "./test-files/Profese_Pridelene_AD_vymazat_orgunitu_MOD_WHERE_EXISTS__utf8-sig.sql"
        # source_sql = "./test-files/Program_garant_pocet_programu_sloucenych__utf8-sig.sql"
        # source_sql = "./test-files/Rozvrh_vyucovani_nesloucene_mistnosti_Apollo__utf8-sig.sql"
        # source_sql = "./test-files/Rozvrh_vyucovani_nesloucene_mistnosti_Apollo_MOD__utf8-sig.sql"
        encoding = "utf-8-sig"
        # source_sql = "./test-files/Plany_prerekvizity_kontrola__ansi.sql"
        # source_sql = "./test-files/Predmety_planu_zkouska_projekt_vypisovani_vazba_err__ansi.sql"
        # encoding = "ansi"

    exit_code = 0
    f = None
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
        # f = open(source_sql[:-4] + "__vystup.txt", "w")
        # f.write(formatted_sql + "\n")

        # # KOD K BUG REPORTU ( https://github.com/andialbrecht/sqlparse/issues/700 )
        # statement = parse("SELECT LISTAGG(attr,', ') WITHIN GROUP(ORDER BY attr) as column FROM table")
        # print(statement[0].tokens)
        # for t in statement[0].tokens:
        #     try:
        #         print(t.tokens)
        #     except:
        #         pass

        # KOD K BUG REPORTU ( https://github.com/andialbrecht/sqlparse/issues/701 )
        # # s = parse("SELECT column_a AS ca, LISTAGG(column_b, ', ') AS cb, col_c FROM table")
        # query = ("SELECT uid "
        #            ", ROW_NUMBER() OVER ( PARTITION BY table.col_a ORDER BY table.col_b DESC ) as row "
        #            ", full_name AS name "
        #            ", another_column AS acol "
        #            ", ROW_NUMBER() OVER ( PARTITION BY table.col_c ORDER BY table.col_d ASC ) as row_lo "
        #            ", ROW_NUMBER() OVER ( PARTITION BY table.col_e ORDER BY table.col_f DESC ) as row_hi "
        #            ", last_column AS lc "
        #            "FROM table")
        # statement = parse(query)
        # print(statement[0].tokens)
        # for t in statement[0].tokens:
        #     print(t.value)
        #     try:
        #         print(t.tokens)
        #     except:
        #         pass

        statements = parse(query, encoding=encoding)
        for s in statements:
            process_statement(s)

        for table in Table.__tables__:
            # Potlacime vypis tabulek, ktere nejsou docasne (= existuji v DB a jsou tedy referencovany stylem schema.tabulka)
            if "." in table.name:
                continue
            output = f"{table}\n"
            print(output)
            # # DEBUG: obcas se hodi ukladat vystup konzoly i na disk...
            # f.write(output + "\n")
    except:
        print("\nDOŠLO K CHYBĚ:\n\n" + traceback.format_exc())
        exit_code = 1
    finally:
        if f != None:
            f.close()
    os._exit(exit_code)  # sys.exit(exit_code) nelze s exit_code > 0 pouzit -- vyvola dalsi vyjimku (SystemExit)
