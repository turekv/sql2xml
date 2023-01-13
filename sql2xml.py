import sqlparse.sql as sql
from sqlparse import format, parse
import sys
import os
import traceback


class Attribute:    
    def __init__(self, name, alias=None, condition=None):
        self.name = name
        self.alias = alias
        # self.aliases = []
        # if alias != None:
        #     self.aliases.append(alias)
        self.condition = condition


class Table:
    __next_id__ = 0
    __next_select_num__ = 0
    __next_join_num__ = 0
    __next_generic_num__ = 0
    __tables__ = []

    def __init__(self, name=None, name_template=None, alias=None, attributes=None, comment="", source_sql=""):
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
        self.comment = comment
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
                attribute_collection.append(f"{attr.name}{condition}{alias}")
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
        max_snippet_length = 50
        if len(self.comment) < max_snippet_length:
            comment = self.comment
        else:
            comment = self.comment[:(max_snippet_length - 6)] + " [...]"
        comment = comment.replace("\n", " ")
        if len(self.source_sql) < max_snippet_length:
            source_sql = self.source_sql
        else:
            source_sql = self.source_sql[:(max_snippet_length - 6)] + " [...]"
        source_sql = source_sql.replace("\n  ", " ")
        return f"TABULKA {self.name} (ID {self.id})\n{indent}Aliasy:\n{indent}{indent}{aliases}\n{indent}Attributy:\n{indent}{indent}{attributes}\n{indent}Vazba na tabulky:\n{indent}{indent}{names}\n{indent}Komentář:\n{indent}{indent}\"{comment}\"\n{indent}SQL kód:\n{indent}{indent}\"{source_sql}\""
    
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
    def __generate_name__(cls, name_template):
        if name_template == "select":
            name = f"select-{Table.__next_select_num__}"
            Table.__next_select_num__ += 1
        elif name_template == "join":
            name = f"join-{Table.__next_join_num__}"
            Table.__next_join_num__ += 1
        else:
            name = f"table-{Table.__next_generic_num__}"
            Table.__next_generic_num__ += 1
        return name
    
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
                    add_attrib = False
                    break
            if add_attrib:
                new_attributes.append(a)
        self.attributes.extend(new_attributes)


def get_name_and_alias(t):
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
    return name, alias


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
    return Attribute(name=name, condition=f"{operator} {value}")


def get_attribute_conditions(t):

    # TODO: zatim ignoruje logicke spojky mezi podminkami -- je toto ale nutne resit?

    attributes = []
    if isinstance(t, sql.Parenthesis) or isinstance(t, sql.Where):
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
                while token != None:
                    if token.ttype == sql.T.Keyword:
                        components.append(token.normalized)
                    elif token.ttype in sql.T.Literal:
                        components.append(token.value)
                    else:
                        break
                    (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
                value = " ".join(components)
                attributes.append(Attribute(name=name, condition=f"{operator} {value}"))
            
            # TODO: comment na konci?

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
                else:
                    break
                (i, token) = t.token_next(i, skip_ws=True, skip_cm=False)
            value = " ".join(components)
            attributes.append(Attribute(name=name, condition=f"{operator} {value}"))
    
        # TODO: comment na konci?
    
    return attributes


def process_with_element(t):
    # Struktura t.tokens: Identifier [ [ whitespace(s) ] Punctuation [ whitespace(s) ] Identifier [ ... ] ]
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
        if (isinstance(last_token, sql.Comment)
                or last_token.ttype == sql.T.Comment.Single
                or last_token.ttype == sql.T.Comment.Multiline):
            comment = last_token.value
        else:
            comment = ""
        table = Table(name=name, comment=comment, source_sql=t.value.strip())
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


        # TODO: zajima nas i comment NAD definici bloku, ktery ale je predchozim tokenem!


def process_token(t, is_within=None):
    # print(f"TOKEN (ttype: {t.ttype}, class: {type(t).__name__}, is_keyword: {t.is_keyword}, is_group: {t.is_group}):\n  {t}\n")
    if is_within == "select":
        attributes = []
        if isinstance(t, sql.Identifier):
            name, alias = get_name_and_alias(t)
            attributes.append(Attribute(name=name, alias=alias))
        elif isinstance(t, sql.IdentifierList):
            for token in t.tokens:
                if isinstance(token, sql.Identifier):
                    name, alias = get_name_and_alias(token)
                    attributes.append(Attribute(name=name, alias=alias))
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
            Table.__tables__.append(table)
            process_statement(t.tokens[0], table)
            return table
        else:
            return get_name_and_alias(t)
    # if is_within == "join":
    #     return get_name_and_alias(t)
    if is_within == "with":
        if isinstance(t, sql.Identifier):
            process_with_element(t)
        elif isinstance(t, sql.IdentifierList):
            for token in t.tokens:
                process_with_element(token)
        return None
    if is_within == "on":
        return get_attribute_conditions(t)
    if isinstance(t, sql.IdentifierList):
        for token in t:
            process_token(token, is_within)
        return None
        

def process_statement(s, table=None, known_attribute_aliases=False):
    # CTE ... Common Table Expression (WITH, ...)
    # DDL ... Data Definition Language (...)
    # DML ... Data Manipulation Language (SELECT, ...)

    # TODO: komentare mozna bude lepsi ukladat ve stylu comment_before, comment_after...? (= castecne duplicitne)

    i = 0
    t = s.token_first(skip_ws=True, skip_cm=False)
    is_within = None
    last_comment = ""
    # Zdrojovy kod:
    #   * WITH: lze primo pomoci t.value
    #   * JOIN: nutno skladat po castech (oddelene tokeny)
    #   * SELECT: u "( SELECT ... )" sice lze pouzit t.parent.value, ale toto u top-level SELECT (bez uvedeni v zavorkach) ulozi vzdy kompletne cely (!) SQL dotaz, coz neni zadouci. I zde tedy jsou zdrojove kody skladany po castech.
    sql_components = []
    while t != None:
        # Nestaci testovat pouze isinstance(t, Comment)
        if (isinstance(t, sql.Comment)
                or t.ttype == sql.T.Comment.Single
                or t.ttype == sql.T.Comment.Multiline):
            last_comment = t.value
            
            # TODO: DORESIT

        elif (t.ttype == sql.T.Keyword and t.normalized == "GROUP BY"
                or t.ttype == sql.T.Keyword and t.normalized == "ORDER BY"):
            # Pri nalezeni klicovych slov GROUP BY, ORDER BY preskocime nasledujici token

            # TODO: LIMIT? OFFSET? DESC? jina klicova slova?

            # TODO: klicova slova mohou mit vicero parametru --> nestaci vzdy preskocit pouze jeden nasl. token!!! TEDY: jaky typ tokenu je nutno najit, nez lze pokracovat v analyze dotazu?

            sql_components.append(t.value)
            (i, t) = s.token_next(i, skip_ws=True, skip_cm=False)
        elif t.ttype == sql.T.CTE and t.normalized == "WITH":
            is_within = "with"
        elif t.ttype == sql.T.DML and t.normalized == "SELECT":
            is_within = "select"
            # Pokud jde o SELECT na nejvyssi urovni dotazu, neexistuje pro nej zatim zadna tabulka. Tuto tedy vytvorime, aby k ni pak bylo mozne doplnit atributy atd.
            if table == None:
                table = Table(name_template="select")
                Table.__tables__.append(table)
            sql_components = []
        elif t.ttype == sql.T.Keyword and t.normalized == "FROM":
            is_within = "from"
        elif t.ttype == sql.T.Keyword and "JOIN" in t.normalized:
            is_within = "join"
            sql_components = []
        elif isinstance(t, sql.Where):
            table.update_attributes(get_attribute_conditions(t))
        elif t.ttype == sql.T.Keyword and t.normalized == "ON":
            is_within = "on"
        elif not t.ttype == sql.T.Punctuation:
            obj = process_token(t, is_within)
            if obj != None:
                if isinstance(obj, list) and isinstance(obj[0], Attribute):
                    if is_within == "on":
                        join_table.update_attributes(obj)

                        # TODO: mozna updatovat attribs v OBOU tabulkach z JOIN? (pozor: nelze podle LHS/RHS -- bylo by potreba delat podle referenci na tabulky v nazvech atributu)

                        sql_components.append(t.value)
                        join_table.source_sql = "\n  ".join(sql_components).strip()
                    elif known_attribute_aliases:
                        if len(obj) < len(table.attributes):
                            raise(f"Počet aliasů atributů v tabulce {table.name} je větší než počet hodnot vracených příkazem SELECT")
                        # Vime, ze aliasy atributu tabulky ve WITH musely byt uvedeny ve stejnem poradi jako atributy nyni zjistene z prikazu SELECT
                        for i in range(len(table.attributes)):
                            table.attributes[i].name = obj[i].name
                            table.attributes[i].condition = obj[i].condition  # TODO: mozna neni potreba? (attrib conditions jsou nastavovany pouze v pripade JOIN)
                        # pridame pripadne dalsi atributy, ktere byly zjisteny nad ramec aliasu uvedenych za nazvem tabulky
                        for i in range(len(table.attributes), len(obj)):
                            table.attributes.append(obj[i])
                    else:
                        table.attributes.extend(obj)
                elif isinstance(obj, tuple) and isinstance(obj[0], str):
                    # Najdeme zdrojovou tabulku, odkud se berou data, a pridame k ni alias
                    src_table = Table.get_table_by_name(obj[0])
                    if src_table == None:
                        src_table = Table(name=obj[0], alias=obj[1])
                        Table.__tables__.append(src_table)
                    else:
                        src_table.add_alias(obj[1])
                    # Pokud aktualne resime JOIN, vytvorime patricnou "mezitabulku", ke ktere budou nasledne nastaveny podminky dle ON
                    if is_within == "join":
                        join_table = Table(name_template="join")
                        Table.__tables__.append(join_table)
                        # Zavislosti: table --> join_table --> src_table
                        table.link_to_table_id(join_table.id)
                        join_table.link_to_table_id(src_table.id)
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
            is_within = None
        sql_components.append(t.value)
        (i, t) = s.token_next(i, skip_ws=True, skip_cm=False)
    # Obsah sql_components se resetuje pri nalezeni SELECT, resp. JOIN. Pokud je SELECT v zavorkach ("SELECT ... FROM ( SELECT ... )"), obsahuje kolekce na konci jednu uzaviraci zavorku navic, kterou je potreba odebrat.
    if len(sql_components) > 0 and sql_components[0].lower() == "select":
        if sql_components[-1] == ")":
            sql_components.pop()
        table.source_sql = "\n  ".join(sql_components).strip()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        source_sql = str(sys.argv[0])
        encoding = str(sys.argv[1])
    else:
        # print("\nSyntaxe:\n\n  sql2xml SOUBOR KODOVANI\n\nkde:\n  SOUBOR    cesta k souboru s SQL dotazem\n  KODOVANI  kódování, které má být použito při čtení souboru výše\n            (ansi, utf-8 apod.)\n")
        # os._exit(1)  # sys.exit(1) vyvola dalsi vyjimku (SystemExit)!

        # DEBUG
        # source_sql = "./test-files/EI_znamky_2F_a_3F__utf8.sql"
        source_sql = "./test-files/sql_parse_pokus__utf8.sql"
        # source_sql = "./test-files/Plany_prerekvizity_kontrola__utf8.sql"
        encoding = "utf-8"
        # source_sql = "./test-files/Plany_prerekvizity_kontrola__ansi.sql"
        # encoding = "ansi"
    
    try:
        print()
        with open(source_sql, mode="r", encoding=encoding) as file:
            query = "".join(file.readlines())
        
        # VYPSANI PUVODNIHO DOTAZU V PREFORMATOVANEM STAVU
        # S komentari neni idealni (nektera zalomeni radku jsou orezana apod.)
        # print(f"\nPŘEFORMÁTOVANÝ DOTAZ (s komentáři):\n-----------------------------------\n{format(query, encoding=encoding, reindent=True, keyword_case='upper', strip_comments=False)}\n")
        # Bez komentaru
        print(f"\nPŘEFORMÁTOVANÝ DOTAZ (bez komentářů):\n-------------------------------------\n{format(query, encoding=encoding, reindent=True, keyword_case='upper', strip_comments=True)}\n-------------------------------------\n")

        statements = parse(query, encoding=encoding)
        for s in statements:
            process_statement(s)
        
        for table in Table.__tables__:
            print(f"{table}\n")
    except:
        print("\nDOŠLO K CHYBĚ:\n\n" + traceback.format_exc())
        os._exit(1)  # sys.exit(1) nelze pouzit -- vyvola dalsi vyjimku (SystemExit)
