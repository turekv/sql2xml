/* Profese pridelene - AD_GROUP = 1 (pracovni tymy) s null orgunitou
*/
--merge into apolloadm.ap_pers_prof pp_dest
--using
--    (
------ Cast vyse musi byt nad WITH.
with
    --------------------------- Pridelene AD profese, rn=1 nejstarsi zaznam s disable_auto_remove
    prof_tmp as (select pp.*
            , row_number() over ( partition by pp.id_profession, pp.id_person 
                                  order by pp.disable_auto_remove desc, pp.ins_ts asc, pp.orgunitid) as rn 
        from apolloadm.ap_profession pr                                                 -- *** Ap_profession
            inner join apolloadm.ap_pers_prof pp                                        -- *** Ap_pers_prof
                  on ( pp.id_profession = pr.id_profession
                       and pr.ad_group = 1 -- Pouze pracovni tymy
                       and pr.type != 'Z'  -- Bez profesi spisove sluzby
                       and pp.status = 9
                       and pr.status = 9
                     )
    ) -- as prof_tmp     --------------------------- Pridelene AD profese, rn=1  nejstarsi zaznam s disable_auto_remove
    
--select tmp.* from prof_tmp tmp 
--where 
--     tmp.orgunitid is not null
--     and 
--    tmp.id_profession = 35418
----    and tmp.id_person = 88707
----order by rn desc
--/*

    --------------------------- Nove hodnoty
    , prof_new as (select
            pt.* 
--            pt.id_pers_prof, pt.id_person, pt.orgunitid
--            , pt.disable_auto_remove, pt.status, pt.rn
            , decode(pt.rn, 1,null, pt.orgunitid) as orgunitid_new -- Nejvhodnejsi zaznam - vymazat orgunitu
            , decode(pt.rn, 1,pt.status, -1) as status_new -- Ostatni zaznamy - smazat statusem
--            , pt.id_profession
            
        from prof_tmp pt                                                                -- *** Prof_tmp
    ) -- as prof_new     --------------------------- Nove hodnoty
    
--select tmp.* from prof_new tmp
--/*
---- Zmenene udaje + ty, ktere pro stejnou osobu a profesi zustavaji nezmeneny
select tmp.* from prof_new tmp
where exists
    ( select tmp2.status
      from prof_new tmp2
      where ( nvl(tmp2.orgunitid,-1) != nvl(tmp2.orgunitid_new,-1)     -- Pouze kdyz je co zmenit
               or tmp2.status != tmp2.status_new 
            )
            and tmp2.id_profession = tmp.id_profession  -- Vazba ven
            and tmp2.id_person = tmp.id_person          -- Vazba ven
    )
order by tmp.id_profession, tmp.id_person, tmp.rn
/*

---- Select pro merge - Pouze kdyz je co zmenit
select tmp.id_pers_prof, tmp.orgunitid_new, tmp.status_new, tmp.id_profession
from prof_new tmp
where  nvl(tmp.orgunitid,-1) != nvl(tmp.orgunitid_new,-1)     -- Pouze kdyz je co zmenit
    or tmp.status != tmp.status_new
--/*
    ) pp_src
    on (pp_dest.id_pers_prof = pp_src.id_pers_prof)
when matched then update set
    pp_dest.orgunitid = pp_src.orgunitid_new
    , pp_dest.status = pp_src.status_new
    , pp_dest.upd_ts = sysdate
    , pp_dest.upd_uid = 2749



-- */