# -*- coding: utf-8 -*-
"""
Created on Wed Sep 19 08:18:32 2018

@author: eliu
"""

import os
from Util.propagate import *
from Dataloaders.get_returns import *


class Portfolio(object):

    __slots__ = ('name', 'description', 'author', 'calendar', 'life', 'base_currency', 'data_location',
                 'position_frequency', 'position_forward_fill_flag', 'position_forward_fill_days',
                 'benchmark', 'long_short_flag', 'renormalization_flag')

    def __init__(self,
                 name=None,
                 description=None,
                 author=None,
                 calendar='US',
                 life=(19000101, 99991231),
                 base_currency='USD',
                 data_location=None,
                 position_frequency='DAILY',
                 position_forward_fill_flag=None,
                 position_forward_fill_days=None,
                 benchmark=None,
                 long_short_flag=None,
                 renormalization_flag=None):

        if name is not None and isinstance(name, str):
            self.name = name

        if description is not None and isinstance(description, str):
            self.description = description

        if author is not None and isinstance(author, str):
            self.author = author

        if calendar is not None and isinstance(calendar, str):
            self.calendar = calendar

        if life is not None and isinstance(life, tuple):
            self.life = life

        if base_currency is not None and isinstance(base_currency, str):
            self.base_currency = base_currency

        if data_location is not None and isinstance(data_location, str):
            self.data_location = data_location

        if position_frequency is not None and isinstance(position_frequency, str):
            self.position_frequency = position_frequency

        if position_forward_fill_flag is not None and isinstance(position_forward_fill_flag, bool):
            self.position_forward_fill_flag = position_forward_fill_flag

        if position_forward_fill_days is not None and isinstance(position_forward_fill_days, (int, float)):
            self.position_forward_fill_days = position_forward_fill_days

        if benchmark is not None and isinstance(benchmark, (int, str)):
            self.benchmark = benchmark

        if long_short_flag is not None and isinstance(long_short_flag, bool):
            self.long_short_flag = long_short_flag

        if renormalization_flag is not None and isinstance(renormalization_flag, bool):
            self.renormalization_flag = renormalization_flag

    def get_positions(self, startdt, enddt, calendar=None, forward_fill_flag=None,
                      forward_fill_days=None, recurse=None):

        result = {'portfolio_name': [None], 'dates': [None], 'values': [None],
                  'master_ident': [None], 'master_values': [None]}

        if calendar is None or not isinstance(calendar, str):
            calendar = self.calendar

        if forward_fill_flag is None or not isinstance(forward_fill_flag, bool):
            forward_fill_flag = self.position_forward_fill_flag

        if forward_fill_days is None or not isinstance(forward_fill_days, (int, float)):
            forward_fill_days = self.position_forward_fill_days

        if recurse is None or not isinstance(recurse, bool):
            recurse = False

        por_days = load_trading_days(calendar, startdt, enddt)
        T = len(por_days)
        if T == 0:
            warnings.warn('No valid trading days with calendar %s' % calendar)
            return None

        result = {'dates': por_days, 'values': np.array([None] * T), 'master_ident': [None]}
        for i in range(T):
            d = por_days[i]
            file = "%s\\%s.dat" % (self.data_location, d.strftime('%Y%m%d'))
            if not os.path.isfile(file):
                continue
            try:
                data = load_data(file)
                val = {'dates': d, 'ident': data['ident'], 'values': data['values']}

                #  recurse?

                result['values'][i] = val
                result['master_ident'] = np.union1d(result['master_ident'], result['values'][i]['ident'])
            except ValueError:
                warnings.warn('Unable to load universe composition on %s' % d.strftime('%Y%m%d'))
                continue

        result['portfolio_name'] = self.name
        result['calendar'] = calendar

        if self.renormalization_flag:
            for i in range(len(result['dates'])):
                p = result['values']['i']
                if not p or not p['ident']:
                    continue
                if self.long_short_flag:
                    p['values'] = p['values'] / np.nansum(p['values'][p['values'] > 0])
                else:
                    p['values'] = p['values'] / np.nansum(p['values'])
                result['values'][i] = p
                del p

        if forward_fill_flag:
            c = np.isnan(result['values'].astype(float))
            missing = np.where(c)[0]
            por_dates = result['dates']
            for j in range(len(missing)):
                i = missing[j]
                if i == 0:
                    continue
                val = result['values'][i - 1]
                if not val or 'ident' not in val or not val['ident']:
                    continue
                pd = por_dates[i - 1]
                td = por_dates[i]
                try:
                    val = propagate_weights(pd, td, val, calendar)
                    result['values'][i] = val
                    print('Propagated weights %s: from %s - %s') % \
                        (self.name, pd.strftime('%Y%m%d'), td.strftime('%Y%m%d'))
                except ValueError:
                    warnings.warn('Unable to propagate holdings')

        result['master_values'] = np.full((len(result['dates']), len(result['master_ident'])), 0.0)
        for i in range(len(result['dates'])):
            if not result['values'][i] or 'ident' not in result['values'][i] or \
                    not result['values'][i]['ident']:
                continue
            c, ia, ib = intersect(result['master_ident'], result['values'][i]['ident'])
            if not c:
                continue
            result['master_values'][i, ia] = result['values'][i]['values'][ib]
            del (c, ia, ib)

        return result

    def compute_returns(self, startdt, enddt, save_flag=None, calendar=None, output_location=None):

        if calendar is None or not isinstance(calendar, str):
            calendar = self.calendar

        days = load_trading_days(calendar, startdt, enddt)
        if not days:
            warnings.warn('No valid business days according to %s' % calendar)
            return None

        if save_flag is None or not isinstance(save_flag, bool):
            save_flag = False

        if output_location is None or not isinstance(output_location, str):
            output_location = self.data_location

        if save_flag and not os.path.exists(output_location):
            os.makedirs(output_location, exist_ok=True)

        result = {'dates': days, 'portfolio_name': self.name,
                  'values': np.array([np.nan] * len(days)),
                  'hedged_values': np.array([np.nan] * len(days)),
                  'long': np.array([np.nan] * len(days)),
                  'short': np.array([np.nan] * len(days)),}

        all_days = load_trading_days(calendar)
        beg_indx = np.where(all_days == days[0])[0][0]
        end_indx = np.where(all_days == days[-1])[0][0]
        por_days = all_days[beg_indx - 1: end_indx - 1]
        ret_days = all_days[beg_indx: end_indx]
        del (beg_indx, end_indx)

        if not por_days:
            warnings.warn('No valid position dates: %s calendar' % calendar)
            return None

        try:
            por = self.get_positions(por_days[0], por_days[-1], calendar)
        except ValueError:
            raise Exception('Unable to compute or load portfolio positions')

        try:
            ret = get_sec_returns(ret_days[0], ret_days[-1], por['master_ident'], calendar, self.base_currency)
        except ValueError:
            raise Exception('Unable to load returns')

        for i in range(len(days)):
            bday = days[i]
            bdstr = bday.strftime('%Y%m%d')
            pdinx = np.where(por['dates'] < bday)[0]
            if not pdinx:
                warnings.warn('No valid position dates found for return date %s' % bdstr)
                continue
            pdinx = pdinx[-1]
            
            rdinx = np.where(ret['dates'] == bday)[0]
            if not rdinx:
                warnings.warn('No valid return dates found for return date %s' % bdstr)
                continue
            rdinx = rdinx[-1]
            
            p = por['values'][pdinx]
            if not p or not p['ident']:
                warnings.warn('%s: missnig holdings; skipping' % bdstr)
                continue
            rmat = np.array(np.nan * len(p['ident']))
            c, i1, i2 = intersect(p['ident'], ret['ident'])
            rmat[:, i1] = ret['values'][rdinx, i2]
            if 'local_values' in ret:
                lmat = np.array(np.nan * len(p['ident']))
                lmat[:, i1] = ret['local_values'][rdinx, i2]
            else:
                lmat = rmat
            del (c, i1, i2)
            rvec = np.prod(1 + rmat, axis=0) - 1
            lvec = np.prod(1 + lmat, axis=0) - 1
            rcontrib = np.nansum(p['values'] * rvec)
            lcontrib = np.nansum(p['values'] * lvec)
            result['values'][i] = rcontrib
            result['hedged_values'][i] = lcontrib

            long_indx = np.where(p['values'] > 0)[0]
            short_indx = np.where(p['values'] < 0)[0]
            if np.size(long_indx) > 0:
                result['long'][i] = np.nansum(p['values'][long_indx]) * rvec[long_indx] / \
                                       np.nansum(p['values'][long_indx])
            del long_indx
            if np.size(short_indx) > 0:
                result['short'][i] = np.nansum(p['values'][short_indx]) * rvec[short_indx] / \
                                       np.nansum(p['values'][short_indx])
            del short_indx
            del (p, rmat, lmat, rvec, rcontrib, lcontrib)

        if save_flag:
            RESULT = result
            file = "%s\\RETURN_%s.dat" % (output_location, self.name)
            if os.path.isfile(file):
                try:
                    data = load_data(file)
                    ulags = np.union1d(RESULT['lags'], data['lags'])
                    udates = np.union1d(RESULT['dates'], data['dates'])
                    values = np.full((len(udates), len(ulags)), np.nan)
                    hvalues = np.full((len(udates), len(ulags)), np.nan)
                    lvalues = np.full((len(udates), len(ulags)), np.nan)
                    svalues = np.full((len(udates), len(ulags)), np.nan)
                    c, ia, ib = intersect(ulags, data['lags'])
                    c, ic, id = intersect(ulags, RESULT['lags'])
                    c, ie, ig = intersect(udates, data['dates'])
                    c, ih, ii = intersect(udates, RESULT['dates'])

                    values[np.ix_(ie, ia)] = data['values'][np.ix_(ig, ib)]
                    values[np.ix_(ih, ic)] = RESULT['values'][np.ix_(ii, id)]
                    if 'hedged_values' in data:
                        hvalues[np.ix_(ie, ia)] = data['hedged_values'][np.ix_(ig, ib)]
                    hvalues[np.ix_(ih, ic)] = RESULT['hedged_values'][np.ix_(ii, id)]
                    if 'long' in data:
                        lvalues[np.ix_(ie, ia)] = data['long'][np.ix_(ig, ib)]
                    lvalues[np.ix_(ih, ic)] = RESULT['long'][np.ix_(ii, id)]
                    if 'short' in data:
                        svalues[np.ix_(ie, ia)] = data['short'][np.ix_(ig, ib)]
                    svalues[np.ix_(ih, ic)] = RESULT['short'][np.ix_(ii, id)]
                    del(c, ia, ib, ic, id, ie, ig, ih, ii)

                    RESULT['lags'] = ulags
                    RESULT['values'] = values
                    RESULT['hedged_values'] = hvalues
                    RESULT['long'] = lvalues
                    RESULT['short'] = svalues
                    del (values, hvalues, lvalues, svalues)
                except ValueError:
                    warnings.warn('Unable to merge; overwriting')

            try:
                save_data(RESULT, file)
                print('%s successfully saved to %s' % (self.name, file))
            except ValueError:
                warnings.warn('Unable to save results %s' % file)
        return result

    def get_returns(self, startdt, enddt, return_lags=None, data_location=None, calendar=None,
                    base_currency=None, excess_flag=None):

        if calendar is None or not isinstance(calendar, str):
            calendar = self.calendar

        days = load_trading_days(calendar, startdt, enddt)
        T = len(days)
        if T == 0:
            warnings.warn('No valid trading days with calendar %s' % calendar)
            return None

        if return_lags is None or not isinstance(return_lags, (int, float, list, np.ndarray)):
            return_lags = [1]

        if data_location is None or not isinstance(data_location, str):
            data_location = self.data_location

        if not os.path.exists(data_location):
            raise Exception('%s does not exist' % data_location)

        if base_currency is None or not isinstance(base_currency, str):
            base_currency = 'USD'

        if excess_flag is None or not isinstance(excess_flag, bool):
            excess_flag = False

        H = len(return_lags)
        result = {'portfolio_name': self.name, 'dates': days,
                  'values': np.full((T, H), np.nan), 'lags': return_lags}

        file = "%s\\RETURN_%s.dat" % (data_location, self.name)
        if not os.path.isfile(file):
            warnings.warn('%s does not exist' % file)
            return result

        try:
            data = load_data(file)
            c, i1, i2 = intersect(result['dates'], data['dates'])
            c, i3, i4 = intersect(result['lags'], data['lags'])
            result['values'][np.ix_(i1, i3)] = data['values'][np.ix_(i2, i4)]
            del (c, i1, i2, i3, i4)
        except ValueError:
            raise Exception('Unable to load portfolio returns: %s' % self.name)

        # deal with fx

        if excess_flag:
            result['total_returns'] = result['values']
            result['benchmark_returns'] = np.full(result['values'].shape, np.nan)
            if not self.benchmark:
                return result
            all_days = load_trading_days(calendar, None, result['dates'][-1])
            i1 = np.where(days <= result['dates'][0])[0][-1]
            all_days = all_days[i1 - np.max(result['lags']) + 1:]
            del i1
            bmk_ret = get_index_returns(all_days[0], all_days[-1], self.benchmark, calendar, base_currency)
            if not bmk_ret or not bmk_ret['values']:
                return result
            # if bmk is cash and return has missing values, forward fill 5 days
            i1 = np.where(bmk_ret['dates'] == result['dates'][0])[0][0]
            for i in range(len(result['lags'])):
                z = np.exp(moving_sum(np.log(1 + bmk_ret['values'][i1 - result['lags'][i] + 1:]),
                                      result['lags'][i])) - 1
                z = z[result['lags'][i]:]
                result['values'][:, i] = result['total_returns'][:, i] - z
                result['benchmark_returns'][:, i] = z
        return result
