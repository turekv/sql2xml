import sqlparse.sql as sql
from sqlparse import format, parse
import sys
import os
import traceback


class Attribute:
    def __init__(self, name, alias=None, condition=None, comment=None):
        self.name = name
        self.alias = alias
        # self.aliases = []
        # if alias != None:
        #     self.aliases.append(alias)
        self.condition = condition
        if comment != None:
            comment = comment.strip()
        self.comment = comment


class Table:
    __next_id__ = 0
    __next_template_num__ = {}
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
            comment = comment.strip()
        self.comment = comment
        if source_sql != None:
            source_sql = source_sql.strip()
        self.source_sql = source_sql
        self.linked_to_tables_id = []

    def __str__(self):
        indent = "    "
        if len(self.aliases) > 0:
            self.aliases.sort()
            aliases = f"\n{indent}{indent}".join(self.aliases)
        else:
            aliases = "<žádné>"
        if len(self.attributes) > 0:
            attribute_collection = []
            for attr in self.attributes:
                # if len(attr.aliases) > 0:
                #     attr.aliases.sort()
                #     attr_aliases = ", ".join(attr.aliases)
                #     attribute_collection.append(f"{attr.name} (aliasy: {attr_aliases})")
                if attr.condition != None:
                    condition = f" {attr.condition}"
                else:
                    condition = ""
                if attr.alias != None:
                    alias = f" (alias: {attr.alias})"
                else:
                    alias = ""
                if attr.comment != None and len(attr.comment) > 0:
                    attr_comment = f"\n{indent}{indent}{indent}Komentář: \"{Table.__trim_to_length__(attr.comment)}\""
                else:
                    attr_comment = ""
                attribute_collection.append(f"{attr.name}{condition}{alias}{attr_comment}")
            attribute_collection.sort()
            attributes = f"\n{indent}{indent}".join(attribute_collection)
        else:
            attributes = "<žádné>"
        # if len(self.linked_to_tables_id) > 0:
        #     self.linked_to_tables_id.sort()
        #     linked_ids = ", ".join(str(id) for id in self.linked_to_tables_id)
        # else:
        #     linked_ids = "<žádné>"
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
    def __trim_to_length__(cls, text):
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
    def add_alias_to_table(cls, id, alias):
        if id < 0 or alias == None:
            return False
        for table in Table.__tables__:
            if id == table.id:
                return table.add_alias(alias)
        return False

    @classmethod
    def get_table_by_name(cls, name):
        if name == None:
            return None
        for table in Table.__tables__:
            if (name == table.name or name in table.aliases):
                return table
        return None

    @classmethod
    def get_table_by_id(cls, id):
        if id == None or id < 0:
            return None
        for table in Table.__tables__:
            if id == table.id:
                return table
        return None

    @classmethod
    def __generate_id__(cls):
        id = Table.__next_id__
        Table.__next_id__ += 1
        return id

    @classmethod
    def __generate_name__(cls, template):
        if template == None:
            template = "table"
        else:
            template = template.strip().replace(" ", "-")
            if len(template) == 0:
                template = "table"
        try:
            num = Table.__next_template_num__[template]
        except:
            num = 0
        Table.__next_template_num__[template] = num + 1
        return f"{template}-{num}"

    def add_alias(self, alias):
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

    def link_to_table_id(self, id: int):
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

    def update_attributes(self, attributes):
        new_attributes = []
        for a in attributes:
            add_attrib = True
            for ta in self.attributes:
                if (a.name == ta.name
                        or a.name == ta.alias
                        or a.alias == ta.name
                        or ((a.alias != None or ta.alias != None) and a.alias == ta.alias)):
                    ta.condition = a.condition
                    ta.comment = a.comment
                    add_attrib = False
                    break
            if add_attrib:
                new_attributes.append(a)
        self.attributes.extend(new_attributes)


def is_comment(t):
    return (isinstance(t, sql.Comment)
        or t.ttype == sql.T.Comment.Single
        or t.ttype == sql.T.Comment.Multiline)


def get_name_alias_comment(t):
    # Struktura: name [ whitespace(s) [ AS whitespace(s) ] alias ]
    # kde "name" muze byt Identifier, prip. Function
    i = 0
    components = []
    while i < len(t.tokens) and not t.tokens[i].is_whitespace:
        components.append(t.tokens[i].value)
        i += 1
    name = "".join(components)
    while (i < len(t.tokens) and (t.tokens[i].is_whitespace
            or (t.tokens[i].ttype == sql.T.Keyword and t.tokens[i].normalized == "AS"))):
        i += 1
    alias = None
    components = []
    while i < len(t.tokens) and not t.tokens[i].is_whitespace:
        components.append(t.tokens[i].value)
        i += 1
    if len(components) > 0:
        alias = "".join(components)
    last_token = t.tokens[-1]
    if is_comment(last_token):
        return name, alias, last_token.value
    else:
        return name, alias, None


def process_comparison(t):
    components = []
    j = 0
    while j < len(t.tokens) and not t.tokens[j].is_whitespace:
        components.append(t.tokens[j].value)
        j += 1
    name = "".join(components)
    while j < len(t.tokens) and t.tokens[j].ttype != sql.T.Comparison:
        j += 1
    operator = t.tokens[j].normalized.upper()  # Jinak by napr. "in" bylo malymi pismeny
    j += 1
    while j < len(t.tokens) and t.tokens[j].is_whitespace:
        j += 1
    components = []
    while j < len(t.tokens) and not t.tokens[j].is_whitespace:
        components.append(t.tokens[j].value)
        j += 1
    value = "".join(components)
    last_token = t.tokens[-1]
    if is_comment(last_token):
        return Attribute(name=name, condition=f"{operator} {value}", comment=last_token.value)
    else:
        return Attribute(name=name, condition=f"{operator} {value}")


def get_attribute_conditions(t):

    # TODO: zatim ignoruje logicke spojky mezi podminkami -- je toto ale nutne resit?

    attributes = []
    if isinstance(t, sql.Parenthesis) or isinstance(t, sql.Where):
        last_token = t.tokens[-1]
        i = 0
        token = t.token_first(skip_ws=True, skip_cm=False)
        while token != None:
            if isinstance(token, sql.Comparison):
                attributes.append(process_comparison(token))
                (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
            elif isinstance(token, sql.Identifier):
                name = token.value
                (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
                operator = token.normalized
                (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
                components = []
                comment = None
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
            elif is_comment(token):
                if token == last_token:
                    # Zde jsme narazili na komentar k JOIN tabulce ("JOIN ... ON ( ... ) komentar") -- byva uvedeno uplne na konci t.tokens. Pridame fiktivni atribut (name == alias == condition == None, comment != None), ze ktereho pak bude komentar extrahovan a prirazen k tabulce
                    attributes.append(Attribute(name=None, alias=None, condition=None, comment=token.value))
                    return attributes
                elif len(attributes) > 0:
                    # Jde o komentar k poslednimu nalezenemu atributu
                    attributes[-1].comment = token.value.strip()
                    (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
            else:
                (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
    else:
        if isinstance(t, sql.Comparison):
            attributes.append(process_comparison(t))
        elif isinstance(t, sql.Identifier):
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


def process_with_element(t, comment_before=""):
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
    # print(f"TOKEN (ttype: {t.ttype}, class: {type(t).__name__}, is_keyword: {t.is_keyword}, is_group: {t.is_group}):\n  {t}\n")
    if "select" in is_within:
        attributes = []
        if isinstance(t, sql.Identifier):
            name, alias, comment = get_name_alias_comment(t)
            attributes.append(Attribute(name=name, alias=alias, comment=comment))
        elif isinstance(t, sql.IdentifierList):
            for token in t.tokens:
                if isinstance(token, sql.Identifier):
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
            process_with_element(t, comment_before)
        elif isinstance(t, sql.IdentifierList):
            for token in t.tokens:
                comment_before = process_with_element(token, comment_before)
        return comment_before
    if is_within == "on":
        return get_attribute_conditions(t)
    if isinstance(t, sql.IdentifierList):

        # TODO: kdy dojde na tuto cast kodu? (drive mozna bylo potreba, nyni se zda byt zbytecne)
        # TODO: ma vubec smysl tady resit predavani komentare?

        for i in range(len(t.tokens)):
            if i == 0:
                process_token(t.tokens[i], is_within, comment_before)
            else:

                # TODO: zde bude asi nutne aktualizovat comment_before podle toho, co je v t.tokens?

                process_token(t.tokens[i], is_within)
        # for token in t:
        #     process_token(token, is_within)
        return None


def process_statement(s, table=None, known_attribute_aliases=False):
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
            else:
            # elif t.normalized == "GROUP BY" or t.normalized == "ORDER BY":
                # Pri nalezeni klicovych slov preskocime nasledujici token

                # TODO: klicova slova dost mozna mohou mit vicero parametru --> nemusi vzdy stacit preskocit pouze jeden nasl. token!

                sql_components.append(t.value)
                (i, t) = s.token_next(i, skip_ws=True, skip_cm=False)
        elif t.ttype == sql.T.CTE and t.normalized == "WITH":
            is_within = "with"
        elif t.ttype == sql.T.DML and t.normalized == "SELECT":
            if is_within == "union-select":
                # Pokud SELECT nasleduje po UNION [ALL], musime pro toto vytvorit tabulku (spojované SELECTy mohou byt vc. WHERE apod. a slouceni vsech atributu takovych SELECTu pod nadrazenou tabulku by nemuselo davat smysl). K tomuto pripadu tedy je nutne pristupovat podobně jako k JOIN.
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
            if union_table != None:
                union_table.update_attributes(attributes)
            else:
                table.update_attributes(attributes)
        elif not t.ttype == sql.T.Punctuation:
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

                        # TODO: mozna updatovat attribs v OBOU tabulkach z JOIN? (pozor: nelze podle LHS/RHS -- bylo by potreba delat podle referenci na tabulky v nazvech atributu)

                        sql_components.append(t.value)
                        join_table.source_sql = "\n".join(sql_components).strip()
                    elif is_within == "union-select":
                        union_table.attributes.extend(obj)
                    elif known_attribute_aliases:
                        if len(obj) < len(table.attributes):
                            raise(f"Počet aliasů atributů v tabulce {table.name} je větší než počet hodnot vracených příkazem SELECT")
                        # Vime, ze aliasy atributu tabulky ve WITH musely byt uvedeny ve stejnem poradi jako atributy nyni zjistene z prikazu SELECT
                        for i in range(len(table.attributes)):
                            table.attributes[i].name = obj[i].name
                            table.attributes[i].condition = obj[i].condition  # TODO: mozna neni potreba? (attrib conditions jsou nastavovany pouze v pripade JOIN)
                            table.attributes[i].comment = obj[i].comment
                        # pridame pripadne dalsi atributy, ktere byly zjisteny nad ramec aliasu uvedenych za nazvem tabulky
                        for i in range(len(table.attributes), len(obj)):
                            table.attributes.append(obj[i])
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
                        # Pokud aktualne resime JOIN, vytvorime patricnou "mezitabulku", ke ktere budou nasledne nastaveny podminky dle ON
                        join_table = Table(name_template="join")
                        Table.__tables__.append(join_table)
                        # Zavislosti: table --> join_table --> src_table
                        table.link_to_table_id(join_table.id)
                        join_table.link_to_table_id(src_table.id)
                    elif union_table != None:
                        # pokud resime UNION, je nutne vytvorit "mezitabulku" podobně jako v pripade JOIN
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
        # source_sql = "./test-files/EI_znamky_2F_a_3F__utf8.sql"
        # source_sql = "./test-files/PHD_studenti_SDZ_SZZ_predmety_publikace__utf8.sql"
        # source_sql = "./test-files/Plany_prerekvizity_kontrola__utf8.sql"
        source_sql = "./test-files/Predmety_aktualni_historie__utf8.sql"
        # source_sql = "./test-files/sql_parse_pokus__utf8.sql"
        encoding = "utf-8"
        # source_sql = "./test-files/Predmety_literatura_pouziti_v_planech_Apollo__utf8-sig.sql"
        # source_sql = "./test-files/Profese_Pridelene_AD_vymazat_orgunitu__utf8-sig.sql"
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

        statements = parse(query, encoding=encoding)
        for s in statements:
            process_statement(s)

        for table in Table.__tables__:
            output = f"{table}\n"
            print(output)
            # f.write(output + "\n")
    except:
        print("\nDOŠLO K CHYBĚ:\n\n" + traceback.format_exc())
        os._exit(1)  # sys.exit(1) nelze pouzit -- vyvola dalsi vyjimku (SystemExit)
    finally:
        if f != None:
            f.close()
