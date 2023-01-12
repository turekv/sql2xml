/* Pokusny kralik parsovani
*/
with 
    ------------------ Ropg komentar
    ropg as 
    (select ro.rocik_id, ob.obor_id, ro.cislo_rocniku as rocnik, ob.zkratka as ob 
     from st01.obor ob 
          inner join st01.rocnik ro 
                on ro.obor_id = ob.obor_id 
     )
-- select tmp.* from ropg tmp
-- /* 

  ------------------ Cosi komentar
  -- Dalsi vysvetlivka
  , cosi (rocnik_id, semestr_id, typ_semestru_id) as 
    (select ropg.rocnik_id, se.semestr_id, se.typ_semestru_id
            , decode(cts.typ_semestru_id 1,'L', null) as sem 
     from ropg 
          inner join st01.semestr se 
                on se.rocnik_id = ro.rocnik_id
          inner join (select * from st01.c_typ_semestru) cts 
                on cts.typ_semestru_id = se.typ_semestru_id
     where ropg.rocnik_id in (1, 2, 3) 
     ) 
select * from cosi
-- /*



-- */