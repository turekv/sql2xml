import sqlparse.sql as sql
from sqlparse import format, parse
import sys
import os
import traceback


class Attribute:
    def __init__(self, name, alias=None, condition=None, comment=None):
        self.name = name
        self.alias = alias
        self.condition = condition
        if comment != None:
            comment = comment.strip()
        self.comment = comment


class Table:
    """Trida reprezentujici tabulku (kazda tabulka ma unikatni ID a jmeno)"""
    __next_id__ = 0
    __next_template_num__ = {}
    # Kolekce existujicich tabulek
    __tables__ = []

    def __init__(self, name=None, name_template=None, alias=None, attributes=None, comment=None, source_sql=None):
        self.id = Table.__generate_id__()
        if name != None:
            self.name = name
        else:
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
            # Textove reprezentace atributu vc. pripadnych podminek, aliasu a komentaru nejprve ulozime do kolekce (aby pak slo tuto kolekci opet seradit podle abecedy)
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
                attribute_collection.append(f"{attr.name}{condition}{alias}{attr_comment}")
            attribute_collection.sort()
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
        return f"TABULKA {self.name} (ID {self.id})\n{indent}Aliasy:\n{indent}{indent}{aliases}\n{indent}Attributy:\n{indent}{indent}{attributes}\n{indent}Vazba na tabulky:\n{indent}{indent}{names}\n{indent}Komentář:\n{indent}{indent}\"{comment}\"\n{indent}SQL kód:\n{indent}{indent}\"{source_sql}\""
    
    @classmethod
    def __trim_to_length__(cls, text: str) -> str:
        """Zkrati zadany text na max_snippet_length == 50 znaku, prip. vrati puvodni text, pokud byl kratsi. Veskera zalomeni radku jsou zaroven nahrazena mezerami."""
        # Potreba v situaci, kdy napr. sql_source == None
        if text == None:
            return ""
        text = text.replace("\n", " ")
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

    @classmethod
    def get_table_by_name(cls, name: str) -> "Table":
        """Vrati odkaz na tabulku zadaneho jmena, prip. None, pokud v kolekci Table.__tables__ zadna takova tabulka neexistuje. Porovnavani jmen je case-sensitive!"""
        if name == None:
            return None
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

    def add_alias(self, alias: str) -> bool:
        """Zkusi k tabulce pridat zadany alias. Pokud se toto podari, vrati True, jinak (napr. pokud uz zadany alias je mezi znamymi aliasy) vrati False. Porovnavani aliasu je case-sensitive + metoda NEKONTROLUJE pritomnost zadaneho aliasu u zbylych tabulek!"""
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
        components.append(t.tokens[i].value)
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
    while j < len(t.tokens) and not t.tokens[j].is_whitespace:
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
    """Vraci seznam atributu vc. jejich pozadovanych hodnot. Zadany token muze byt jak obycejnym porovnanim (Comparison), tak sekvenci sub-tokenu urcujicich napr. rozmezi hodnot ("rok BETWEEN 2010 AND 2020" apod.)."""

    # TODO: zatim ignoruje logicke spojky mezi podminkami -- je toto ale nutne resit?

    attributes = []
    # Postup se lisi podle toho, zda sqlparse vratil jednoduche srovnani (Comparison), sekvenci tokenu (napr. pro urceni rozmezi hodnot), nebo je toto navic v zavorce (Parenthesis) ci jako soucast WHERE.
    if isinstance(t, sql.Parenthesis) or isinstance(t, sql.Where):
        # Zde mame jeden token a musime projit jeho sub-tokeny (t.tokens)
        # Nejprve si ulozime referenci na posledni token v t.tokens (muze byt komentarem k atributu)
        last_token = t.tokens[-1]
        i = 0
        # Tokeny prochazime iteratorem, kde rovnou preskakujeme bile znaky (ne vsak komentare)
        token = t.token_first(skip_ws=True, skip_cm=False)
        while token != None:
            if isinstance(token, sql.Comparison):
                # Obycejne srovnani zpracujeme metodou process_comparison(...)
                attributes.append(process_comparison(token))
                # Zaroven je potreba precist nasledujici token -- hromadne (za podminkou) toto delat nelze kvuli odlisnemu zpusobu zpracovani samostatne sekvence tokenu atd.
                (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
            elif isinstance(token, sql.Identifier):
                # Zde mame sekvenci tokenu se strukturou "jmeno operator literal ..."
                # Pokud je jmeno zadano s teckami, je toto vraceno jako jediny token (cili lze rovnou vzit hodnotu prvniho tokenu)
                name = token.value
                (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
                operator = token.normalized
                (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
                components = []
                comment = None
                # Ted budeme postupne kontrolovat jednotlive tokeny a pokud narazime na klicove slovo nebo literal, ulozime si jeho hodnotu do components. Pripadny nalezeny komentar si taktez ulozime, jelikoz ho bude nutne predat do konstruktoru atributu.
                while token != None:
                    if token.ttype == sql.T.Keyword:
                        components.append(token.normalized)
                    elif token.ttype in sql.T.Literal:
                        components.append(token.value)
                    elif is_comment(token):
                        comment = token.value
                    else:
                        # Jakmile narazime na jiny typ nez nektery ze tri vyse uvedenych, nemuze uz jit o soucast podminky u atributu a z cyklu tedy muzeme vyskocit
                        break
                    (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
                # Slucujeme klicova slova a literaly --> musime pouzit oddelovac v podobe mezery
                value = " ".join(components)
                # Vysledny atribut ulozime do kolekce attributes
                attributes.append(Attribute(name=name, condition=f"{operator} {value}", comment=comment))
            elif isinstance(token, sql.Parenthesis):
                # Pokud jsme narazili na zavorku, zpracujeme ji rekurzivne a vratime kolekci atributu rozsirime o navracene atributy
                attributes.extend(get_attribute_conditions(token))
                # Nakonec musime prejit na dalsi token
                (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
            elif is_comment(token):
                if token == last_token:
                    # Zde jsme narazili na komentar k JOIN tabulce ("JOIN ... ON ( ... ) komentar") -- byva uvedeno uplne na konci t.tokens. Pridame fiktivni atribut (name == alias == condition == None, comment != None), ze ktereho pak bude komentar extrahovan a prirazen k tabulce
                    attributes.append(Attribute(name=None, alias=None, condition=None, comment=token.value))
                    return attributes
                if len(attributes) > 0:
                    # Jde o komentar k poslednimu nalezenemu atributu
                    attributes[-1].comment = token.value.strip()
                    (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
            else:
                (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
        return attributes
    if isinstance(t, sql.Comparison):
        # Token je obycejnym srovnanim, takze staci do kolekce attributes pridat navratovou hodnotu process_comparison(...) (nelze vratit primo tuto navratpovou hodnotu, tzn. objekt typu Attribute, protoze typ navratove hodnoty se pozdeji poziva k rozliseni, jak presne s takovou hodnotou nalozit)
        attributes.append(process_comparison(t))
        return attributes
    if isinstance(t, sql.Identifier):
        # Zde postupujeme analogicky situaci popsane vyse (Parenthesis/Where). Rozdil je vyhradne v tom, ze kolekce attributes bude ve vysledku obsahovat jediny atribut.
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
    if isinstance(t, sql.Parenthesis):
        # Token je zavorkou, takze ho zpracujeme rekurzivne
        return get_attribute_conditions(t)


def process_with_element(t, comment_before="") -> str:
    """"""
    # Struktura t.tokens: Identifier [ [ whitespace(s) ] Punctuation [ whitespace(s) ] Identifier [ ... ] ]
    comment_after = comment_before  # pokud jsme narazili napr. na Punctuation, musime si komentar ponechat pro referenci
    if isinstance(t, sql.Identifier):
        # Struktura obj.tokens: name whitespace(s) [AS whitespace(s) ] parenthesis-SELECT [ whitespace(s) [ comment ] ]
        # Pokud je uveden jen nazev tabulky, je prvni token typu Name. Jsou-li za nazvem tabulky v zavorkach uvedeny aliasy sloupcu, je prvni token typu Function.
        aliases = []
        if t.tokens[0].ttype == sql.T.Name:
            name = t.tokens[0].value
        else:
            name = t.tokens[0].tokens[0].value
            i = 1
            while i < len(t.tokens[0].tokens) and not isinstance(t.tokens[0].tokens[i], sql.Parenthesis):
                i += 1
            for par_token in t.tokens[0].tokens[i].tokens:
                if isinstance(par_token, sql.Identifier):
                    aliases.append(par_token.value)
                elif isinstance(par_token, sql.IdentifierList):
                    for alias_token in par_token.tokens:
                        if isinstance(alias_token, sql.Identifier):
                            aliases.append(alias_token.value)
        i = 1
        while i < len(t.tokens) and not isinstance(t.tokens[i], sql.Parenthesis):
            i += 1
        # Zpracovani zavorky se SELECT zatim preskocime -- nejprve vyrobime patricnou tabulku, jejich atributy budou potom doplneny
        last_token = t.tokens[-1]
        if is_comment(last_token):
            comment_after = last_token.value.strip()
        else:
            comment_after = ""
        table = Table(name=name, comment=comment_before, source_sql=t.value)
        if len(aliases) > 0:
            known_attribute_aliases = True
            # Zatim nastavime jmena na "TBD" s tim, ze tato budou aktualizovana v process_statement(...) nize
            for a in aliases:
                table.attributes.append(Attribute(name="TBD", alias=a))
        else:
            known_attribute_aliases = False
        Table.__tables__.append(table)
        # Nyni doresime zavorku, odkaz na jiz vytvorenou tabulku predame
        process_statement(t.tokens[i], table, known_attribute_aliases)
    return comment_after


def process_token(t, is_within=None, comment_before=""):
    if is_within != None and "select" in is_within:
        if isinstance(t, sql.Parenthesis):
            table = Table(name_template=is_within, comment=comment_before)
            Table.__tables__.append(table)
            process_statement(t, table)
            return table
        else:
            attributes = []
            if isinstance(t, sql.Identifier):
                name, alias, comment = get_name_alias_comment(t)
                attributes.append(Attribute(name=name, alias=alias, comment=comment))
            elif isinstance(t, sql.IdentifierList):
                for token in t.tokens:
                    if isinstance(token, sql.Identifier) or isinstance(token, sql.Function):
                        name, alias, comment = get_name_alias_comment(token)
                        attributes.append(Attribute(name=name, alias=alias, comment=comment))
            elif t.ttype == sql.T.Wildcard:
                attributes.append(Attribute(name="*"))
            return attributes
    if is_within == "from" or is_within == "join":
        if isinstance(t.tokens[0], sql.Parenthesis):
            # Struktura t.tokens: parenthesis-SELECT [ whitespace(s) [AS whitespace(s) ] alias ]
            table = Table(name_template="select")
            i = 1
            while (i < len(t.tokens) and (t.tokens[i].is_whitespace
            or (t.tokens[i].ttype == sql.T.Keyword and t.tokens[i].normalized == "AS"))):
                i += 1
            components = []
            while i < len(t.tokens) and not t.tokens[i].is_whitespace:
                components.append(t.tokens[i].value)
                i += 1
            if len(components) > 0:
                table.add_alias("".join(components))
            last_token = t.tokens[-1]
            # Komentar pridame jen v pripade, ze zatim neni nastaveny
            if (is_comment(last_token)
                    and (table.comment == None or len(table.comment) == 0)):
                table.comment = last_token.value.strip()
            Table.__tables__.append(table)
            process_statement(t.tokens[0], table)
            return table
        else:
            return get_name_alias_comment(t)
    if is_within == "with":
        if isinstance(t, sql.Identifier):
            comment_before = process_with_element(t, comment_before)
        elif isinstance(t, sql.IdentifierList):
            for token in t.tokens:
                comment_before = process_with_element(token, comment_before)
        return comment_before
    if is_within == "on":
        return get_attribute_conditions(t)
    # if isinstance(t, sql.Identifier):

    #     # TODO: muze tento pripad vubec nastat? (drive mozna bylo potreba, nyni se zda byt zbytecne)

    #     process_token(t.tokens[i], is_within, comment_before)
    #     return None
    #     # return get_name_alias_comment(t)
    
    # if isinstance(t, sql.IdentifierList):

    #     # TODO: muze tento pripad vubec nastat? (drive mozna bylo potreba, nyni se zda byt zbytecne)
    #     # TODO: ma vubec smysl tady resit predavani komentare?

    #     for i in range(len(t.tokens)):
    #         if i == 0:
    #             process_token(t.tokens[i], is_within, comment_before)
    #         else:

    #             # TODO: zde bude asi nutne aktualizovat comment_before podle toho, co je v t.tokens?

    #             process_token(t.tokens[i], is_within)
    #     # for token in t:
    #     #     process_token(token, is_within)
    #     return None


def process_statement(s, table=None, known_attribute_aliases=False):

    # TODO: doresit referencovani stejnych tabulek pomoci formalne ruznych jmen (napr. pap_tmp vs. st01.pap_tmp)

    # CTE ... Common Table Expression (WITH, ...)
    # DDL ... Data Definition Language (...)
    # DML ... Data Manipulation Language (SELECT, ...)
    i = 0
    t = s.token_first(skip_ws=True, skip_cm=False)
    is_within = None
    comment_before = ""
    token_counter = 10  # Lib. hodnota takova, aby se v cyklu na zacatku NEresetoval comment_before, pokud by SQL dotaz nezacinal komentarem
    # Zdrojovy kod:
    #   * WITH: lze primo pomoci t.value
    #   * JOIN: nutno skladat po castech (oddelene tokeny)
    #   * SELECT: u "( SELECT ... )" sice lze pouzit t.parent.value, ale toto u top-level SELECT (bez uvedeni v zavorkach) ulozi vzdy kompletne cely (!) SQL dotaz, coz neni zadouci. I zde tedy jsou zdrojove kody skladany po castech.
    sql_components = []
    # union_* jsou potreba v pripade, ze sjednocovani je provadeno bez prikazu "SELECT ..." bez zavorek (tzn. "SELECT ... UNION SELECT ..."), jelikoz pak je patricny SQL kod vracen jako prosta sekvence tokenu). Pokud je nektery SELECT v zavorkach, zpracovava se jako samostatny statement.
    union_components = []
    union_table = None
    while t != None:
        token_counter += 1
        if token_counter == 2:
            comment_before = ""
        if is_comment(t):
            comment_before = t.value.strip()
            token_counter = 0
        elif t.ttype == sql.T.Keyword:
            if t.normalized == "FROM":
                is_within = "from"
            elif "JOIN" in t.normalized:
                is_within = "join"
                sql_components = []
                if union_table != None:
                    union_table.source_sql = "\n".join(union_components).strip()
                    union_table = None
            elif t.normalized == "ON":
                is_within = "on"
            elif "UNION" in t.normalized:
                is_within = "union-select"
            
            # POCKAT NA VYRESENI BUG REPORTU ( https://github.com/andialbrecht/sqlparse/issues/701 )
            # elif t.normalized == "OVER":
            #     # Klicove slovo OVER a nasledna zavorka s pripadnym PARTITION BY apod. jsou vraceny jako dva tokeny oddelene od predchoziho tokenu s funkci (de facto nazvem atributu). Pripadny alias a komentar jsou az soucasti tokenu se zavorkou. Prvni token s OVER tedy pridame do sql_components a nasledne z druheho tokenu zjistime pripadny alias a komentar.
            #     # Samotny obsah zavorky za OVER nas pritom v zasade nezajima, jelikoz urcuje razeni zaznamu a zde zminene atributy se ve vracenem datasetu vubec nemusi vyskytovat.
            #     sql_components.append(t.value)
            #     (i, t) = s.token_next(i, skip_ws=True, skip_cm=False)

            #     # TODO: ma smysl zde resit token_counter a comment_before ?

            #     # token_counter += 1       # Je potreba? Neslo by proste na konci tohoto bloku
            #     # if token_counter == 2:   # nastavit comment_before = "" ?
            #     #     comment_before = ""  #

            #     # Struktura t.tokens:
            #     #   * pokud t je typu Identifier (tzn. "problemovy atribut" je poslednim v seznamu): Parenthesis [ whitespace(s) [ AS whitespace(s) Identifier ] ] [ whitespace(s) Comment ]
            #     #   * pokud t je typu IdentifierList (tzn. za "problemovym atributem" je uveden jeste alespon jeden dalsi): Identifier [ whitespace(s) ] Punctuation [ whitespace(s) ] Identifier ...

            #     # TODO: nebo toto cele resit standardne nize? Jak ale potom predat info o tom, ze se jedna o veci k OVER? (ZDE NEMUZEME PREPSAT/ZMENIT is_within!)

            #     # TODO: POZOR: pokud po sobe nasleduje vice "problematickych atributu" s OVER, je t typu IdentifierList s prvkem Function, ktery analogicky zahrnuje veskery po nem nasledujici kod --> mozna bude potreba tokens = t.flatten() a pak postupne iterovat skrz tokens?

            #     if isinstance(t, sql.Identifier):
            #         obj = process_token(t, "over")

            #         # TODO: zpracovat vraceny objekt -- zde nanejvys alias + komentar k poslednimu nalezenemu atributu

            #     elif isinstance(t, sql.IdentifierList):
            #         for token in t.tokens:
            #             obj = process_token(token, "over")  # TODO: pouzit "over" i zde, kdyz se dale mohou vyskytovat standardni tokeny (Identifier) predstavujici atributy?

            #             # TODO: zpracovat vraceny objekt
            
            else:
                # Pri nalezeni klicovych slov preskocime nasledujici token (aktualne se zda, ze u syntakticky spravneho SQL dotazu je i skupina parametru klicoveho slova vracena jako jeden token (IdentifierList, Parenthesis, ...))
                sql_components.append(t.value)
                (i, t) = s.token_next(i, skip_ws=True, skip_cm=False)
        elif t.ttype == sql.T.CTE and t.normalized == "WITH":
            is_within = "with"
        elif t.ttype == sql.T.DML and t.normalized == "SELECT":
            if is_within == "union-select":
                # Spojovane SELECTy mohou byt vc. WHERE apod. a slouceni vsech atributu takovych SELECTu pod nadrazenou tabulku by nemuselo davat smysl. Pokud tedy po UNION [ALL] nasleduje SELECT (bez uvedeni v zavorkach), musime pro toto uz zde vytvorit tabulku. Je tedy nutne pristupovat podobně jako u JOIN. Je-li SELECT v zavorkach, zpracuje se dale jako samostatny statement.
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
            # Struktura t.tokens: Where whitespace(s) [ Parenthesis | Comparison | Identifier | Exists ] [ ... ]
            # Nejprve musime zjistit, jestli jde o obycejne WHERE, nebo o WHERE EXISTS. Toto lze provest nejjednoduseji tak, ze najdeme druhy token v poradi (pri preskakovani mezer/... a komentaru) a zkontrolujeme, zda jde o EXISTS.
            # t.token_first(skip_ws=True, skip_cm=True)  # neni potreba
            (j, token) = t.token_next(0, skip_ws=True, skip_cm=True)
            if token.ttype == sql.T.Keyword and token.normalized == "EXISTS":
                # Nyni cteme dalsi tokeny, ale uz nepreskakujeme komentare (protoze pokud by tam nejaky byl, slo by o komentar k tabulce "WHERE EXISTS ( SELECT ... )").
                (j, token) = t.token_next(j, skip_ws=True, skip_cm=False)
                # Pokud by komentar byl pred WHERE, slo by o komentar k tabulce ve FROM. Ma-li jit o komentar k tabulce WHERE EXISTS ( SELECT ... ), mel by tento byt bud za EXISTS, nebo pred SELECT v zavorce. Je proto potreba zde resetovat comment_before.
                comment_before = ""
                while token != None:
                    if is_comment(token):
                        comment_before = token.value.strip()
                        token_counter = 0
                    elif isinstance(token, sql.Parenthesis):
                        exists_table = Table(name_template="where-exists-select", comment=comment_before)
                        Table.__tables__.append(exists_table)
                        table.link_to_table_id(exists_table.id)
                        process_statement(token, exists_table)
                        # Odsud nelze vyskocit pomoci break, nebot v t.tokens muze za zavorkou jeste byt uveden komentar, ktery ale patri k nasledujicimu tokenu...
                    (j, token) = t.token_next(j, skip_ws=True, skip_cm=False)
            else:
                attributes = get_attribute_conditions(t)
                if union_table != None:
                    union_table.update_attributes(attributes)
                else:
                    table.update_attributes(attributes)
        elif not t.ttype == sql.T.Punctuation:

            # TODO: POZOR: sqlparse neumi WITHIN GROUP(...) (napr. "SELECT LISTAGG(pt.typ_program,', ') WITHIN GROUP(ORDER BY pt.typ_program) AS programy FROM ...") --> odeslan bug report ( https://github.com/andialbrecht/sqlparse/issues/700 )
            # MOZNA BUDE NUTNE OSETRIT NEJAK RUCNE?
            # if t.value.lower().endswith("within"):
            #     # ZDE NELZE (t je casto IdentifierList apod.) -- nutno az v process_token
            #     partial_attr_name = t.value
            #     (i, t) = s.token_next(i, skip_ws=True, skip_cm=False)

            obj = process_token(t, is_within, comment_before)
            if obj != None:
                if isinstance(obj, list) and isinstance(obj[0], Attribute):
                    if is_within == "on":
                        last_attribute = obj[-1]
                        if (last_attribute.name == None
                                and last_attribute.alias == None
                                and last_attribute.condition == None
                                and last_attribute.comment != None):
                            join_table.comment = last_attribute.comment
                            obj.pop()
                        join_table.update_attributes(obj)

                        # TODO: mozna updatovat attributy v OBOU tabulkach z JOIN? (pozor: nelze podle LHS/RHS -- bylo by potreba delat podle referenci na tabulky v nazvech atributu)

                        sql_components.append(t.value)
                        join_table.source_sql = "\n".join(sql_components).strip()
                    elif is_within == "union-select":
                        union_table.attributes.extend(obj)
                    elif known_attribute_aliases:
                        if len(obj) < len(table.attributes):
                            raise(f"Počet aliasů atributů v tabulce {table.name} je větší než počet hodnot vracených příkazem SELECT")
                        # Vime, ze aliasy atributu tabulky ve WITH musely byt uvedeny ve stejnem poradi jako atributy nyni zjistene z prikazu SELECT
                        for j in range(len(table.attributes)):
                            table.attributes[j].name = obj[j].name
                            table.attributes[j].condition = obj[j].condition  # TODO: mozna neni potreba? (attrib conditions jsou nastavovany pouze v pripade JOIN)
                            table.attributes[j].comment = obj[j].comment
                        # Pridame pripadne dalsi atributy, ktere byly zjisteny nad ramec aliasu uvedenych za nazvem tabulky
                        for j in range(len(table.attributes), len(obj)):
                            table.attributes.append(obj[j])
                    else:
                        table.attributes.extend(obj)
                elif isinstance(obj, tuple) and isinstance(obj[0], str):
                    # Najdeme zdrojovou tabulku, odkud se berou data, a pridame k ni alias
                    src_table = Table.get_table_by_name(obj[0])
                    if src_table == None:
                        src_table = Table(name=obj[0], alias=obj[1], comment=obj[2])
                        Table.__tables__.append(src_table)
                    else:
                        src_table.add_alias(obj[1])
                        # Komentar pridame jen v pripade, ze zatim neni nastaveny
                        if src_table.comment == None or len(src_table.comment) == 0:
                            src_table.comment = obj[2]
                    if is_within == "join":
                        # Pokud resime JOIN, vytvorime patricnou "mezitabulku", ke ktere budou nasledne nastaveny podminky dle ON
                        join_table = Table(name_template="join")
                        Table.__tables__.append(join_table)
                        # Zavislosti: table --> join_table --> src_table
                        table.link_to_table_id(join_table.id)
                        join_table.link_to_table_id(src_table.id)
                    elif union_table != None:
                        # Pokud resime UNION, je nutne vytvorit "mezitabulku" podobně jako v pripade JOIN
                        # Zavislosti: table --> union_table --> src_table
                        table.link_to_table_id(union_table.id)
                        union_table.link_to_table_id(src_table.id)
                    else:
                        table.link_to_table_id(src_table.id)
                elif isinstance(obj, Table):
                    if is_within == "join":
                        join_table = Table(name_template="join")
                        Table.__tables__.append(join_table)
                        # Zavislosti: table --> join_table --> src_table
                        table.link_to_table_id(join_table.id)
                        join_table.link_to_table_id(obj.id)
                    # elif is_within == "union-select":
                    #     table.link_to_table_id(obj.id)
                    else:
                        table.link_to_table_id(obj.id)
                elif is_within == "with" and isinstance(obj, str):
                    comment_before = obj
                    token_counter = 0
            is_within = None
        sql_components.append(t.value)
        union_components.append(t.value)
        (i, t) = s.token_next(i, skip_ws=True, skip_cm=False)
    # Obsah sql_components se resetuje pri nalezeni SELECT, resp. JOIN. Pokud je SELECT v zavorkach ("SELECT ... FROM ( SELECT ... )"), obsahuje kolekce na konci jednu uzaviraci zavorku navic, kterou je potreba odebrat.
    if len(sql_components) > 0 and sql_components[0].lower() == "select":
        if sql_components[-1] == ")":
            sql_components.pop()
        table.source_sql = "\n".join(sql_components).strip()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        source_sql = str(sys.argv[0])
        encoding = str(sys.argv[1])
    else:
        # print("\nSyntaxe:\n\n  sql2xml SOUBOR KODOVANI\n\nkde:\n  SOUBOR    cesta k souboru s SQL dotazem\n  KODOVANI  kódování, které má být použito při čtení souboru výše\n            (ansi, utf-8 apod.)\n")
        # os._exit(1)  # sys.exit(1) vyvola dalsi vyjimku (SystemExit)!

        # DEBUG
        source_sql = "./test-files/EI_znamky_2F_a_3F__utf8.sql"
        # source_sql = "./test-files/PHD_studenti_SDZ_SZZ_predmety_publikace__utf8.sql"
        # source_sql = "./test-files/Plany_prerekvizity_kontrola__utf8.sql"
        # source_sql = "./test-files/Predmety_aktualni_historie__utf8.sql"
        # source_sql = "./test-files/sql_parse_pokus__utf8.sql"
        encoding = "utf-8"
        # source_sql = "./test-files/Predmety_literatura_pouziti_v_planech_Apollo__utf8-sig.sql"
        # source_sql = "./test-files/Profese_Pridelene_AD_vymazat_orgunitu__utf8-sig.sql"
        # source_sql = "./test-files/Profese_Pridelene_AD_vymazat_orgunitu_MOD_WHERE_EXISTS__utf8-sig.sql"
        # source_sql = "./test-files/Program_garant_pocet_programu_sloucenych__utf8-sig.sql"
        # encoding = "utf-8-sig"
        # source_sql = "./test-files/Plany_prerekvizity_kontrola__ansi.sql"
        # source_sql = "./test-files/Predmety_planu_zkouska_projekt_vypisovani_vazba_err__ansi.sql"
        # encoding = "ansi"

    f = None
    try:
        print()
        with open(source_sql, mode="r", encoding=encoding) as file:
            query = "".join(file.readlines())

        # VYPSANI PUVODNIHO DOTAZU V PREFORMATOVANEM STAVU
        # S komentari neni idealni (nektera zalomeni radku jsou orezana apod.)
        # print(f"\nPŘEFORMÁTOVANÝ DOTAZ (s komentáři):\n-----------------------------------\n{format(query, encoding=encoding, reindent=True, keyword_case='upper', strip_comments=False)}\n")
        # Bez komentaru
        formatted_sql = f"\nPŘEFORMÁTOVANÝ DOTAZ (bez komentářů):\n-------------------------------------\n{format(query, encoding=encoding, reindent=True, keyword_case='upper', strip_comments=True)}\n-------------------------------------\n"
        print(formatted_sql)
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
            output = f"{table}\n"
            print(output)
            # f.write(output + "\n")
    except:
        print("\nDOŠLO K CHYBĚ:\n\n" + traceback.format_exc())
    finally:
        if f != None:
            f.close()
    os._exit(1)  # sys.exit(1) nelze pouzit -- vyvola dalsi vyjimku (SystemExit)
