# encoding: utf-8
from threading import RLock
import json
import re
import urllib2
import functools
import datetime as dt
import pandas as pd
import src.core.constant as cst
import src.core.account as acnt
import src.tradecommon.constant as com_cst
import src.tradecommon.variables as com_var
import src.tradecommon.collector as com_collect
import src.tradecommon.calculator as com_calc
import src.mktinfo.stock as mkt_sk
import src.mktinfo.constant as mkt_cst
import src.mktinfo.base as mkt_base
from src.db.config import J_CFG
from src.utils.strcodec import to_utf8
import src.core.saveload as sl
from src.utils.rounding import i_round_number, ROUND_FLOOR, ROUND_HALF_UP
from src.utils.tradeday import get_tday_util
from src.core.compare import FloatCompareStaff

_mkt_info_equity_adj_lock = RLock()
_SUCCESS_ADJ_CACHE_DICT = J_CFG['quotesinfo'].table('equity_adj')['success_result_cache']
_SUCCESS_ADJ_CACHE_PATH_FMT = to_utf8(_SUCCESS_ADJ_CACHE_DICT['path'])
_SUCCESS_ADJ_CACHE_CSV_ENCODING = _SUCCESS_ADJ_CACHE_DICT['encoding']
_EQUITY_EVENTS_CFG_TBL_NAME = 'equityevents'
_TEN = 10
_TEN_FLOAT = 10.0
_CN_SONG = '送股'
_CN_ZHUAN = '转股'
_CN_PAI = '派息'


class EquityAdjError(ArithmeticError):
    pass


class EquityEventEstimatorBase(object):
    def estimate(self, date, pre_position_df):
        """     
        :param pre_position_df: 
        :return: equity_adj_df
        """
        raise NotImplementedError


class EquityAdjustResultCheckerBase(object):
    def __init__(self, name):
        self._date = None
        self._name = name
        self._pre_position_df = com_cst.FLAG_NO_INPUT
        self._equity_adj_df = com_cst.FLAG_NO_INPUT
        self._market_info_getter = None

    def get_pre_position_df(self):
        return self._pre_position_df

    def set_pre_position_df(self, val):
        if not com_cst.is_null(val):
            self._pre_position_df = val[com_var.StockPositionDF.get_header()].copy()

    pre_position_df = property(get_pre_position_df, set_pre_position_df)

    def get_equity_adj_df(self):
        return self._equity_adj_df

    def set_equity_adj_df(self, val):
        if not com_cst.is_null(val):
            self._equity_adj_df = val[com_var.StockEquityAdjustDF.get_header()].copy()

    equity_adj_df = property(get_equity_adj_df, set_equity_adj_df)

    def get_market_info_getter(self):
        if self._market_info_getter is None:
            with _mkt_info_equity_adj_lock:
                if self._market_info_getter is None:
                    self._market_info_getter = mkt_sk.get_stock_market_info_feeder()
        return self._market_info_getter

    def set_market_info_getter(self, val):
        if not isinstance(val, mkt_sk.StockMarketInfoFeederBase):
            raise TypeError('market info getter must be %s instance', mkt_sk.StockMarketInfoFeederBase.__name__)
        self._market_info_getter = val

    market_info_getter = property(get_market_info_getter, set_market_info_getter)

    def set_date(self, date):
        self._date = date

    def get_date(self):
        if self._date is None:
            self._date = cst.Calender.TODAY
        return self._date

    date = property(get_date, set_date)

    def _check_adj_impl(self):
        """  
        :return: 
            success_equity_adj, error_msg
        """
        raise NotImplementedError

    def check(self):
        success_equity_adj, error_msg = self._check_adj_impl()
        if com_cst.is_null(success_equity_adj):
            success_equity_adj = com_var.StockEquityAdjustDF.empty_df()
        equity_adj_result_path = dt.datetime.now().strftime(_SUCCESS_ADJ_CACHE_PATH_FMT.format(self._name))
        success_equity_adj.to_csv(equity_adj_result_path,
                                  index=False, encoding=_SUCCESS_ADJ_CACHE_CSV_ENCODING)
        if error_msg:
            print "equity adj result has been saved in ", equity_adj_result_path
            raise EquityAdjError(to_utf8(error_msg))
        return True


class EquityAdjustResultByPercentChangeChecker(EquityAdjustResultCheckerBase):

    def __init__(self, *args, **kwargs):
        super(EquityAdjustResultByPercentChangeChecker, self).__init__(*args, **kwargs)
        self._cmp_staff = FloatCompareStaff(pct_range=0.005, abs_tolerance=1)

    def _check_adj_impl(self):
        if com_cst.is_null(self.pre_position_df):
            return com_cst.FLAG_NO_INPUT, ""
        pre_position_df = self.pre_position_df.copy()
        pre_position_df[mkt_cst.H_SK_PERCENT_CHANGE] = self.market_info_getter.cnf1d1(
            pre_position_df[com_cst.H_STOCK_CODE],
            mkt_cst.H_SK_PERCENT_CHANGE,
            self._date
        )
        pre_position_df = pre_position_df[pre_position_df[mkt_cst.H_SK_PERCENT_CHANGE].notnull()]
        pre_position_df['today_adjfactor'] = self.market_info_getter.cnf1d1(
            pre_position_df[com_cst.H_STOCK_CODE],
            mkt_cst.H_SK_WIND_POST_ADJFACTOR,
            self._date
        )
        pre_position_df['yesterday_adjfactor'] = self.market_info_getter.cnf1d1(
            pre_position_df[com_cst.H_STOCK_CODE],
            mkt_cst.H_SK_WIND_POST_ADJFACTOR,
            get_tday_util().offset(-1, self._date)
        )
        relate_flag = ~cst.is_zero(pre_position_df['today_adjfactor'] - pre_position_df['yesterday_adjfactor'])
        relate_pre_position = pre_position_df[relate_flag]
        relate_codes = set(relate_pre_position[com_cst.H_STOCK_CODE])
        if com_cst.is_null(self._equity_adj_df):
            has_equity_codes = set()
        else:
            has_equity_codes = set(self.equity_adj_df[com_cst.H_CORRESPONDING_STOCK_CODE])
        diff_codes = relate_codes ^ has_equity_codes
        if diff_codes:
            err_msg = "codes diff: %s" % ';'.join(diff_codes)
            return self.equity_adj_df, err_msg
        if not has_equity_codes:
            return com_cst.FLAG_NO_INPUT, ""
        relate_pre_position_dict = {code: df
                                    for code, df in relate_pre_position.groupby(com_cst.H_STOCK_CODE, as_index=False)}
        equity_adj_dict = {code: df
                           for code, df in self.equity_adj_df.groupby(com_cst.H_CORRESPONDING_STOCK_CODE, as_index=False)}
        problem_codes = filter(lambda x: x is not None, [self._compare_preposition_and_equity_adj(
            code,
            relate_pre_position_dict[code],
            equity_adj_dict[code]
        ) for code in relate_codes])
        if problem_codes:
            err_msg = "problem codes: %s" % ';'.join(problem_codes)
        else:
            err_msg = ""
        return self.equity_adj_df, err_msg

    def _compare_preposition_and_equity_adj(self, code, pre_position, equity_adj):
        today_value_by_pre_position_and_pct_chg = com_calc.zero_nan_sum(
            pre_position[com_cst.H_POSITION_VALUE]*pre_position[com_cst.H_LONG_SHORT_ENUM]*(
                1+pre_position[mkt_cst.H_SK_PERCENT_CHANGE]/100.0))
        equity_adj_transaction = com_collect.StockEquityEventAdjustmentCollector.equity_adjust_df_to_pseudo_transaction_stat_df(
            equity_adj
        )
        cash_diff = -1.0*com_calc.zero_nan_sum(
            equity_adj_transaction[com_cst.H_DEAL_TURNOVER]*equity_adj_transaction[com_cst.H_BUY_SELL_ENUM]
        )
        today_position = com_calc.SimpleStockPositionCalculator(
            pre_position,
            equity_adj_transaction,
            close_date=self._date
        ).calculate()
        today_position_value = com_calc.zero_nan_sum(today_position[com_cst.H_POSITION_VALUE])
        if not self._cmp_staff.compare(cash_diff + today_position_value, today_value_by_pre_position_and_pct_chg):
            return code
        else:
            return None


class EasyMoneyEquityEventInfoCrawler(object):
    _FH_COL_CFG_MAP = {
        "fhcode": com_cst.H_STOCK_CODE,
        "fhnamecn": com_cst.H_STOCK_NAME_CN,
        "fhcontent": mkt_cst.H_EE_EM_CONTENT,
        "fhtype": mkt_cst.H_EE_EM_DATE_TYPE
    }
    _REGS = {
        mkt_cst.H_EE_SONG_PER_TEN: re.compile("送(\\d[\\d\\.]*)[^\\d\\.]"),
        mkt_cst.H_EE_ZHUAN_PER_TEN: re.compile("转(\\d[\\d\\.]*)[^\\d\\.]"),
        mkt_cst.H_EE_PAI_PER_TEN: re.compile("派(\\d[\\d\\.]*)[^\\d\\.]")
    }

    def __init__(self, date):
        self._cfg_dict = J_CFG[_EQUITY_EVENTS_CFG_TBL_NAME].table("crawler")["easymoney"]
        self._date = date

    def read(self):
        return self._load_data()[EasyMoneyPaiSongZhuanDayRecordDF.get_header()]

    def _load_data(self):
        url = self._cfg_dict["url"].encode("utf-8").format(
            tbid=self._cfg_dict["tbid_fenhong"],
            date=self._date
        )
        response = urllib2.urlopen(url)
        jsons = response.read().decode(self._cfg_dict["encoding"])[1:-1]
        if self._cfg_dict["flag_empty"] in jsons:
            return pd.DataFrame(columns=EasyMoneyPaiSongZhuanDayRecordDF.get_header())
        data_dict = json.loads(jsons)
        data_df = pd.DataFrame(data_dict).rename(columns={
            self._cfg_dict[key]: self._FH_COL_CFG_MAP[key] for key in self._FH_COL_CFG_MAP.keys()
        })
        for col_name in [com_cst.H_STOCK_NAME_CN, mkt_cst.H_EE_EM_CONTENT, mkt_cst.H_EE_EM_DATE_TYPE]:
            data_df[col_name] = data_df[col_name].map(to_utf8)
        for col_name, reg in self._REGS.items():
            data_df[col_name] = data_df[mkt_cst.H_EE_EM_CONTENT].map(functools.partial(self._parse_one_number, reg))
        return data_df[EasyMoneyPaiSongZhuanDayRecordDF.get_header()]

    def _parse_one_number(self, reg, content):
        searched = reg.search(content)
        if searched is None:
            num_str = 'nan'
        else:
            num_str = searched.group(1)
        return float(num_str)


class PaiSongZhuanDayRecordDF(sl.FixHeaderDataFrameVariable):
    _header_ = [
        com_cst.H_STOCK_CODE,
        mkt_cst.H_EE_PAI_PER_TEN,
        mkt_cst.H_EE_SONG_PER_TEN,
        mkt_cst.H_EE_ZHUAN_PER_TEN
    ]


class EasyMoneyPaiSongZhuanDayRecordDF(mkt_base.MarketInfoSaveLoadVariableMixin, PaiSongZhuanDayRecordDF):
    _header_ = [
        com_cst.H_STOCK_CODE,
        mkt_cst.H_EE_PAI_PER_TEN,
        mkt_cst.H_EE_SONG_PER_TEN,
        mkt_cst.H_EE_ZHUAN_PER_TEN,
        com_cst.H_STOCK_NAME_CN,
        mkt_cst.H_EE_EM_CONTENT,
        mkt_cst.H_EE_EM_DATE_TYPE
    ]

    def filter_ex_rd(self):
        if not com_cst.is_null(self.get_value()):
            self.set_value(
                self._df[self._df[mkt_cst.H_EE_EM_DATE_TYPE].map(lambda x: '除权除息' in to_utf8(x))])
        return self

    def load_from_feeder(self):
        record_df = EasyMoneyEquityEventInfoCrawler(self.date).read()
        from src.db.sqlitecon import df_adjust_type
        self.df = record_df
        self.save()

    @classmethod
    def get_pai_song_zhuan_df_at_date(cls, date):
        return cls(date).try_load().filter_ex_rd().df[PaiSongZhuanDayRecordDF.get_header()]


class PaiSongZhuanEstimator(EquityEventEstimatorBase):

    def __init__(self, load_func):
        self._cache = mkt_base.InfoVarCache(load_func, 5, 10)

    def estimate(self, date, pre_position_df):
        record_df = self._cache.get_info_var(date)
        merged_df = self._merged_position_and_record(pre_position_df, record_df)
        ret_list = []
        for _, merged_series in merged_df.iterrows():
            for make_func in [
                self._make_pai_adj,
                self._make_song_adj,
                self._make_zhuan_adj
            ]:
                adj_result = make_func(merged_series)
                if adj_result:
                    ret_list.append(adj_result)
        return pd.DataFrame(ret_list) if ret_list else com_cst.FLAG_NO_INPUT

    def _merged_position_and_record(self, pre_position_df, record_df):
        return pd.merge(pre_position_df, record_df, how='inner', on=com_cst.H_STOCK_CODE)

    def _make_song_adj(self, merged_series):
        song_per_ten = merged_series[mkt_cst.H_EE_SONG_PER_TEN]
        if not song_per_ten > 0:
            return None
        song_num = i_round_number(merged_series[com_cst.H_POSITION] * song_per_ten / _TEN_FLOAT, rule=ROUND_FLOOR)
        return {
            com_cst.H_STOCK_CODE: merged_series[com_cst.H_STOCK_CODE],
            com_cst.H_STOCK_NAME_CN: merged_series[com_cst.H_STOCK_NAME_CN],
            com_cst.H_BUY_SELL_ENUM: com_cst.BS.BUY,
            com_cst.H_DEAL_NUMBER: song_num,
            com_cst.H_CORRESPONDING_STOCK_CODE: merged_series[com_cst.H_STOCK_CODE],
            com_cst.H_CORRESPONDING_STOCK_NAME_CN: merged_series[com_cst.H_STOCK_NAME_CN],
            com_cst.H_REMARK: _CN_SONG
        }

    def _make_zhuan_adj(self, merged_series):
        zhuan_per_ten = merged_series[mkt_cst.H_EE_ZHUAN_PER_TEN]
        if not zhuan_per_ten > 0:
            return None
        zhuan_num = i_round_number(merged_series[com_cst.H_POSITION] * zhuan_per_ten / _TEN_FLOAT, rule=ROUND_FLOOR)
        return {
            com_cst.H_STOCK_CODE: merged_series[com_cst.H_STOCK_CODE],
            com_cst.H_STOCK_NAME_CN: merged_series[com_cst.H_STOCK_NAME_CN],
            com_cst.H_BUY_SELL_ENUM: com_cst.BS.BUY,
            com_cst.H_DEAL_NUMBER: zhuan_num,
            com_cst.H_CORRESPONDING_STOCK_CODE: merged_series[com_cst.H_STOCK_CODE],
            com_cst.H_CORRESPONDING_STOCK_NAME_CN: merged_series[com_cst.H_STOCK_NAME_CN],
            com_cst.H_REMARK: _CN_ZHUAN
        }

    def _make_pai_adj(self, merged_series):
        pai_per_ten = merged_series[mkt_cst.H_EE_PAI_PER_TEN]
        if not pai_per_ten > 0:
            return None
        pai_num = i_round_number(merged_series[com_cst.H_POSITION] * pai_per_ten * _TEN_FLOAT,
                                 rule=ROUND_FLOOR)  # fen, not yuan
        return {
            com_cst.H_STOCK_CODE: com_cst.CNY_FEN_CODE,
            com_cst.H_STOCK_NAME_CN: com_cst.CNY_FEN_NAME_CN,
            com_cst.H_BUY_SELL_ENUM: com_cst.BS.SELL,
            com_cst.H_DEAL_NUMBER: pai_num,
            com_cst.H_CORRESPONDING_STOCK_CODE: merged_series[com_cst.H_STOCK_CODE],
            com_cst.H_CORRESPONDING_STOCK_NAME_CN: merged_series[com_cst.H_STOCK_NAME_CN],
            com_cst.H_REMARK: _CN_PAI
        }


class EquityAdjustEngine(object):

    def __init__(self, checker):
        self._estimators = []  # type: list[EquityEventEstimatorBase]
        self._checker = checker  # type: EquityAdjustResultCheckerBase

    def add_estimator(self, estimator):
        """
        :type estimator: EquityEventEstimatorBase
        :param estimator: 
        :return: 
        """
        self._estimators.append(estimator)

    def make_adjust(self, date, pre_position, error_pass=False):
        if com_cst.is_null(pre_position):
            return com_cst.FLAG_NO_INPUT
        equity_adj_df = pd.concat(
            [estimator.estimate(date, pre_position) for estimator in self._estimators],
            axis=cst.PDAxis.VERTICAL,
            ignore_index=True
        )
        self._checker.set_equity_adj_df(equity_adj_df)
        self._checker.set_pre_position_df(pre_position)
        self._checker.set_date(date)
        try:
            self._checker.check()
        except EquityAdjError:
            import traceback
            traceback.print_exc()
            if not error_pass:
                raise
        return equity_adj_df


def make_default_equity_adj_engine(name):
    checker = EquityAdjustResultByPercentChangeChecker(name)
    pai_song_zhuan_estimator = PaiSongZhuanEstimator(
        load_func=EasyMoneyPaiSongZhuanDayRecordDF.get_pai_song_zhuan_df_at_date
    )
    engine = EquityAdjustEngine(checker)
    engine.add_estimator(pai_song_zhuan_estimator)
    return engine


if __name__ == '__main__':
    crawler = EasyMoneyEquityEventInfoCrawler(cst.Calender.TODAY)
    print crawler._load_data()
