import sqlparse
import sys
import os
import traceback


class Attribute:    
    def __init__(self, name, alias=None):
        self.name = name
        self.aliases = []
        if alias != None:
            self.aliases.append(alias)


class Table:
    __next_id__ = 0
    __tables__ = []

    def __init__(self, name=None, alias=None, attributes=None, comment="", source_sql=""):
        self.id = Table.__generate_id__()
        if name != None:
            self.name = name
        else:
            self.name = self.generate_name()
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
        if len(self.aliases) > 0:
            self.aliases.sort()
            aliases = ", ".join(self.aliases)
        else:
            aliases = "<none>"
        if len(self.attributes) > 0:
            attribute_collection = []
            for attr in self.attributes:
                if len(attr.aliases) > 0:
                    attr.aliases.sort()
                    attr_aliases = ", ".join(attr.aliases)
                    attribute_collection.append(f"{attr.name} (aliases: {attr_aliases})")
                else:
                    attribute_collection.append(attr.name)
            attribute_collection.sort()
            attributes = "\n    ".join(attribute_collection)
        else:
            attributes = "<none>"
        if len(self.linked_to_tables_id) > 0:
            self.linked_to_tables_id.sort()
            linked_ids = ", ".join(self.linked_to_tables_id)
        else:
            linked_ids = "<none>"
        return f"TABLE {self.name}, ID {self.id}\n  Aliases: {aliases}\n  Attributes:\n    {attributes}\n  Linked to table IDs: {linked_ids}\n  Comment: {self.comment}"
    
    @classmethod
    def add_alias_to_table_name(cls, name, alias):
        if name == alias:
            return False
        for tbl in Table.__tables__:
            if (name == tbl.name
                    or (name in tbl.aliases and alias != tbl.name)):
                return tbl.add_alias(alias)
        return False
    
    def __generate_id__():
        id = Table.__next_id__
        Table.__next_id__ += 1
        return id
    
    def generate_name(self):
        self.name = f"Select-{self.id}"
    
    def add_alias(self, alias):
        if alias in self.aliases:
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
    
    def link_to_table_name(self, name: str):
        id = -1
        for tbl in Table.__tables__:
            if name == tbl.name or name in tbl.aliases:
                id = tbl.id
                break
        if id < 0:
            return False
        self.linked_to_tables_id.append(id)
        return True
    
    # def unlink_from_table_id(self, id):
    #     try:
    #         self.linked_to_tables_id.remove(id)
    #         return True
    #     except:
    #         return False


def get_name_and_alias(t):
    # Struktura: name [ whitespace(s) [ AS whitespace(s) ] alias ]
    # kde "name" muze byt Identifier, prip. Function
    idx = 0
    components = []
    while idx < len(t.tokens) and not t.tokens[idx].is_whitespace:
        components.append(t.tokens[idx].value)
        idx += 1
    name = "".join(components)
    while (idx < len(t.tokens) and (t.tokens[idx].is_whitespace
            or (t.tokens[idx].ttype == sqlparse.sql.T.Keyword and t.tokens[idx].normalized == "AS"))):
        idx += 1
    alias = None
    components = []
    while idx < len(t.tokens):
        components.append(t.tokens[idx].value)
        idx += 1
    if len(components) > 0:
        alias = "".join(components)
    return name, alias


def processToken(t, is_within):
    # print(f"TOKEN (ttype: {t.ttype}, class: {type(t).__name__}, is_keyword: {t.is_keyword}, is_group: {t.is_group}):\n  {t}\n")
    if is_within == "select":
        attributes = []
        if isinstance(t, sqlparse.sql.Identifier):
            attributes.append(Attribute(*get_name_and_alias(t)))
        elif isinstance(t, sqlparse.sql.IdentifierList):
            for obj in t.tokens:
                if isinstance(obj, sqlparse.sql.Identifier):
                    attributes.append(Attribute(*get_name_and_alias(obj)))
        return attributes
    if is_within == "from":
        name, alias = get_name_and_alias(t)
        Table.add_alias_to_table_name(name, alias)
        tbl = Table()
        tbl.generate_name()
        tbl.link_to_table_name(name)
        return tbl
    if is_within == "with":
        if isinstance(t, sqlparse.sql.Identifier):
            pass


        elif isinstance(t, sqlparse.sql.IdentifierList):
            for obj in t.tokens:
                if isinstance(obj, sqlparse.sql.Identifier):
                    
                    # TODO: zajima nas i comment NAD definici bloku, ktery ale je predchozim tokenem!!!
                    
                    # Struktura obj.tokens: name whitespace(s) [AS whitespace(s) ] parenthesis-SELECT [ whitespace(s) [ comment ] ]
                    # kde "name" je Identifier


                    pass

        # TODO bude vracet list[Table]? Nebo jak se vyresi pridani jedno ci vice tabulek?


    if isinstance(t, sqlparse.sql.IdentifierList):
        for st in t:
            processToken(st, is_within)
        return None
        
        


if __name__ == "__main__":
    if len(sys.argv) > 1:
        sourceSQL = str(sys.argv[0])
        encoding = str(sys.argv[1])
    else:
        # print("\nSyntaxe:\n\n  sql2xml SOUBOR KODOVANI\n\nkde:\n  SOUBOR    cesta k souboru s SQL dotazem\n  KODOVANI  kódování, které má být použito při čtení souboru výše\n            (ansi, utf-8 apod.)\n")
        # os._exit(1)  # sys.exit(1) vyvola dalsi vyjimku (SystemExit)!

        # DEBUG
        sourceSQL = "./test-files/EI_znamky_2F_a_3F__utf8.sql"
        # sourceSQL = "./test-files/Plany_prerekvizity_kontrola__utf8.sql"
        encoding = "utf-8"
        # sourceSQL = "./test-files/Plany_prerekvizity_kontrola__ansi.sql"
        # encoding = "ansi"
    
    try:
        print()
        with open(sourceSQL, mode="r", encoding=encoding) as file:
            query = "".join(file.readlines())
        
        # # VYPSANI PUVODNIHO DOTAZU V PREFORMATOVANEM STAVU
        # # Nejprve s komentari...
        # print(f"\nPŘEFORMÁTOVANÝ DOTAZ (s komentáři):\n-----------------------------------\n{sqlparse.format(query, encoding=encoding, reindent=True, keyword_case='upper', strip_comments=False)}\n")
        # # ... pak i bez nich, jelikoz jejich vypis mnohdy neni idealni (nektera zalomeni radku jsou orezana apod.)
        # print(f"\nPŘEFORMÁTOVANÝ DOTAZ (bez komentářů):\n-------------------------------------\n{sqlparse.format(query, encoding=encoding, reindent=True, keyword_case='upper', strip_comments=True)}\n")

        # CTE ... Common Table Expression (WITH, ...)
        # DDL ... Data Definition Language (...)
        # DML ... Data Manipulation Language (SELECT, ...)
        statements = sqlparse.parse(query, encoding=encoding)
        for s in statements:
            idx = 0
            # token_first() vraci pouze token
            t = s.token_first(skip_ws=True, skip_cm=False)
            is_within = None
            last_comment = ""
            last_attibutes = None
            while t != None:
                # Nestaci testovat pouze isinstance(t, Comment)
                if (isinstance(t, sqlparse.sql.Comment)
                        or t.ttype == sqlparse.sql.T.Comment.Single
                        or t.ttype == sqlparse.sql.T.Comment.Multiline):
                    last_comment = t.value
                elif (t.ttype == sqlparse.sql.T.Keyword and t.normalized == "GROUP BY"
                        or t.ttype == sqlparse.sql.T.Keyword and t.normalized == "ORDER BY"):
                    # Pri nalezeni klicovych slov GROUP BY, ORDER BY preskocime nasledujici token

                    # TODO: LIMIT? OFFSET? WHERE? DESC? jina klicova slova? Slo by vyrešit jen kontrolou na typ "Keyword"?

                    # TODO: klicova slova mohou mit vicero parametru --> nestaci vzdy preskocit pouze jeden nasl. token!!! TEDY: jaky typ tokenu je nutno najit, nez lze pokracovat v analyze dotazu?

                    # token_next() vraci tuple
                    (idx, t) = s.token_next(idx, skip_ws=True, skip_cm=False)
                elif t.ttype == sqlparse.sql.T.CTE and t.normalized == "WITH":
                    is_within = "with"
                elif t.ttype == sqlparse.sql.T.DML and t.normalized == "SELECT":
                    is_within = "select"
                elif t.ttype == sqlparse.sql.T.Keyword and t.normalized == "FROM":
                    is_within = "from"
                else:
                    obj = processToken(t, is_within)
                    if obj != None:
                        if isinstance(obj, Table):
                            obj.attributes.extend(last_attibutes)
                            Table.__tables__.append(obj)
                        elif isinstance(obj, list) and isinstance(obj[0], Attribute):
                            last_attibutes = obj
                        else:
                            last_attibutes = None
                    is_within = None


                # token_next() vraci tuple
                (idx, t) = s.token_next(idx, skip_ws=True, skip_cm=False)


            for tbl in Table.__tables__:
                print(f"{tbl}\n")
            

    except:
        print("\nDOŠLO K CHYBĚ:\n\n" + traceback.format_exc())
        os._exit(1)  # sys.exit(1) nelze pouzit -- vyvola dalsi vyjimku (SystemExit)!


# VIZ:
#    * https://stackoverflow.com/questions/22303812/how-to-parse-sql-queries-and-sub-queries-using-sqlparser-into-python
#    * https://stackoverflow.com/questions/72087411/simple-way-to-parse-sql-subqueries
#    * https://stackoverflow.com/questions/69746550/get-query-tree-hierarchy-using-python-sqlparse
#    * SQLGLOT? ( https://stackoverflow.com/questions/67704594/sqlparse-issue-with-the-wildcard-like-condition --> https://github.com/tobymao/sqlglot )
