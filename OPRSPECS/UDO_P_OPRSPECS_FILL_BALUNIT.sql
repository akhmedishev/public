-- Заполнение ПБЕ в проводках во всех организациях с 01.01.2023.
-- ПБЕ подбирается по части значения СД 'Структура расходов' или 'Структура доходов'.
-- Из значения СД исключаются первые три символа. оставшаяся часть, начиная с 4 символа
-- должна равняться первым 17 символам номера счёта дебет проводки.

-- Ахмедишев Р.

declare

  function UDO_GET_MIN_PBE_BY_SUBSTR_PROP
  (
    nCOMPANY   in number,
    sPROP_CODE in varchar2,
    sFIND_VAL  in varchar2
  )
  return number
  is
    retval PKG_STD.tREF;
  begin

    select min(B.RN)
      into retval -- select *
      from DICBUNTS B
      join DOCS_PROPS_VALS DPV
        on DPV.UNIT_RN = B.RN
        and DPV.UNITCODE = 'BalanceUnits'
      join DOCS_PROPS DP
        on DPV.DOCS_PROP_RN = DP.RN
      where DP.CODE = UDO_GET_MIN_PBE_BY_SUBSTR_PROP . sPROP_CODE
        and B.COMPANY = UDO_GET_MIN_PBE_BY_SUBSTR_PROP . nCOMPANY
        --and DPV.COMPANY = nCOMPANY
        and substr(DPV.STR_VALUE, 4) = UDO_GET_MIN_PBE_BY_SUBSTR_PROP . sFIND_VAL;

    return retval;

  end;

  function UDO_F_ECONOPRS_DOC(nECONOPR in number) return varchar2
  is
    retval PKG_STD.tSTRING;
  begin
    select PKG_DOCUMENT.MAKE_NUMBER('', EO.OPERATION_PREF, EO.OPERATION_NUMB, EO.OPERATION_DATE)
      into retval
      from ECONOPRS EO
      where EO.RN = nECONOPR;
    return retval;
  end;

  procedure UDO_P_LOG
  (
    nCOMPANY          in number,
    sTEXT             in varchar2,
    sVAR0             in varchar2 default null,
    sVAR1             in varchar2 default null,
    sVAR2             in varchar2 default null,
    sVAR3             in varchar2 default null,
    sVAR4             in varchar2 default null,
    sVAR5             in varchar2 default null,
    sVAR6             in varchar2 default null,
    sVAR7             in varchar2 default null,
    sVAR8             in varchar2 default null,
    sVAR9             in varchar2 default null
  )
  is
  begin
    PKG_TRACE.REGISTER('UDO_P_OPRSPECS_FILL_BALUNIT', 'nCOMPANY='||nCOMPANY,
      F_FORMAT_MESSAGE_TEXT(sTEXT,null,sVAR0,sVAR1,sVAR2,sVAR3,sVAR4,sVAR5,sVAR6,sVAR7,sVAR8,sVAR9));
  end;

  procedure UDO_P_OPRSPECS_FILL_BALUNIT_AT(nCOMPANY in number, dOPER_DATE_FROM in date)
  is
    pragma autonomous_transaction;
    nBALUNIT_FOUND PKG_STD.tREF;
    nCNT     number := 0;
    nCNT_UPD number := 0;
    nCNT_NF  number := 0;
  begin

    UDO_P_LOG(nCOMPANY, '==%s==', GET_COMPANY_NAME(0, nCOMPANY));

    PKG_CHECK.OFF_('OPRSPECS_CHECK_LINKS');

    for rOPRSPECS in
    (
      select SP.RN, SP.PRN, SP.COMPANY, DB.ACC_NUMBER as sACC_DEBIT
        from OPRSPECS SP
        join DICACCS  DB on SP.ACCOUNT_DEBIT = DB.RN
        where SP.COMPANY = nCOMPANY
          and DB.ACC_BALANCE = 1
          and SP.OPERATION_DATE >= dOPER_DATE_FROM
          and UDO_F_OPERSPEC_IN_ECPDATA(SP.RN) = 0  -- 0="Нет", 2="Ошибка"
          and SP.BALUNIT_DEBIT is null  -- предотвращает повторную обработку
    )
    loop
      --UDO_P_LOG(nCOMPANY, '');
      nCNT := nCNT + 1;
      nBALUNIT_FOUND := UDO_GET_MIN_PBE_BY_SUBSTR_PROP(rOPRSPECS.COMPANY, 'Структура расходов', substr(rOPRSPECS.sACC_DEBIT, 1, 17));
      if nBALUNIT_FOUND is null then
        nBALUNIT_FOUND := UDO_GET_MIN_PBE_BY_SUBSTR_PROP(rOPRSPECS.COMPANY, 'Структура доходов', substr(rOPRSPECS.sACC_DEBIT, 1, 17));
      end if;
      if nBALUNIT_FOUND is null then
        UDO_P_LOG(nCOMPANY, 'ХО %s: Не найдено ПБЕ со значением свойства Структура расходов или Структура доходов: ???%s',
                 UDO_F_ECONOPRS_DOC(rOPRSPECS.PRN), substr(rOPRSPECS.sACC_DEBIT, 1, 17));
        nCNT_NF := nCNT_NF + 1;
      else
        update OPRSPECS SP
          set SP.BALUNIT_DEBIT = nBALUNIT_FOUND,
              SP.BALUNIT_CREDIT = nBALUNIT_FOUND
          where SP.RN = rOPRSPECS.RN;
        nCNT_UPD := nCNT_UPD + sql%rowcount;
      end if;
    end loop;
    UDO_P_LOG(nCOMPANY, 'Количество отобранных проводок с %s: %s.', d2s(dOPER_DATE_FROM), nCNT);
    UDO_P_LOG(nCOMPANY, 'Сколько раз не удалось найти ПБЕ: %s.', nCNT_NF);
    UDO_P_LOG(nCOMPANY, 'Сколько раз ПБЕ записано в проводку: %s.', nCNT_UPD);

    PKG_CHECK.ON_('OPRSPECS_CHECK_LINKS');
    commit;

  end UDO_P_OPRSPECS_FILL_BALUNIT_AT;

begin

  for rCO in (select RN from COMPANIES order by NAME)
  loop
    UDO_P_OPRSPECS_FILL_BALUNIT_AT(rCO.RN, s2d('01.01.2023'));
  end loop;

  --select RN, DATE_STAMP, DATA1, DATA2, TT.ELAPSED_TIME, tsn2s(null, sum(TT.ELAPSED_TIME) over ()) as TOTAL_ELAPSED_TIME from TRACE_TABLE TT where DATA = 'UDO_P_OPRSPECS_FILL_BALUNIT' order by DATE_STAMP, TIME_STAMP, RN;

end;
