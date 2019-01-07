import pandas as pd
import csv
import argparse
import numpy as np

event_log_columns = [
    'version',
    'dateReceived',
    'timeReceived',
    'ipAddress',
    'serverName',
    'servletName',
    'userAgent',
    'operatingSystem',
    'host',
    'sitelet',
    'vurl',
    'siteId',
    'scores',
    'lookupId',
    'limits',
    'action',
    'extAdnetworkId',
    'extAdvertiserId',
    'extCampaignId',
    'extPlanId',
    'extPublisherId',
    'extChannelId',
    'extPlacementId',
    'extPassbackUrl',
    'parseMethod',
    'countryCode',
    'count',
    'reason',
    'stateCode',
    'passbackId',
    'reasonElement',
    'dma',
    'memHits',
    'eventType',
    'userAgentStr',
    'refUrlType',
    'javascriptInfo',
    'platform',
    'isSafe',
    'ipUserAgentHash',
    'ext_bidUrl',
    'ext_bidPrice',
    'ext_uid',
    'ext_impId',
    'sadEvidence',
    'exchangeInfo',
    'macros',
    'cookieValue',
    'diagnosticsField',
    'originalReferringUrl',
    'setBundleId',
    'setSdkAppId',
    'setOriginalIPAddress',
    'extInsertionOrderId',
    'extCampaignGroupId',
    'certifiedSupply',
    'pubOrder',
    'pubCreative',
    'custom',
    'custom2',
    'custom3',
    'override',
    'adServerId',
    'auth',
    'authToken',
    'authFailureReason',
    'setAppStoreTypeCode',
    'setSelectedBundleId',
    'setSelectedBundleIdSource',
    'setIdentifiedBundleIds',
    'setCustomMetricIds'
]

ABF_BOTS_MAP = {}


def read_file(input_file_location, columns):
    dataframe = pd.read_table(input_file_location,
                       sep='\t',
                       header=None,
                       names=columns,
                       low_memory=False,
                       chunksize=100000,  # only read 100,000 rows at a time.
                       infer_datetime_format=True,
                       parse_dates=['dateReceived'])
    print 'loading file %s' % input_file_location
    return dataframe


def setup_args_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--event_log', required=True, help='location of event log file')
    parser.add_argument('--valid_browsers_file', required=True, help='valid browsers file')
    parser.add_argument('--iab_bots_file', required=True, help='iab bots file')
    parser.add_argument('--abf_bots_file', required=True, help='abf bots file location')
    return parser


def get_normalized_date(impression_date):
    return pd.datetime(impression_date.year, impression_date.month, impression_date.day)


def initialize_iab_list(iabFilePath):
    iabList = []

    iabFile = open(iabFilePath, 'r')
    for line in iabFile:
        if not (line.startswith('#')):
            tokens = line.split('|')
            iabList.append(tokens)

    iabFile.close()
    return iabList


def validateUserAgentList(userAgentsList, validBrowserList, botList):
    matchedUserAgents = []

    for userAgent in userAgentsList:
        isValidBrowser = False
        isBot = False

        for entry in validBrowserList:
            if matchValidBrowser(userAgent.lower(), entry):
                isValidBrowser = True
                break

        if (isValidBrowser):
            for entry in botList:
                if matchBot(userAgent.lower(), entry):
                    isBot = True
                    break

        matchedUserAgents.append({'userAgentStr': userAgent, 'isValidBrowser': isValidBrowser, 'isBot': isBot})

    return pd.DataFrame(matchedUserAgents)


def is_passed_impression(row):
    action = row['action']
    override = row['override']
    visibility_score = get_score(row['scores'], 'visibility')
    return (action == 'passed' or action == 'preview') and \
           (override == 'null' or override == 0 or (visibility_score is not None and visibility_score < 1)) \
           and row['isBot'] == False and row['isAbfBot'] == False and row['isValidBrowser'] == True


def matchValidBrowser(userAgent, entry):
    # second field must be 1 for active matching
    if entry[1] == '1':
        # third field == 1 indicates match beginning of string
        if entry[2].rstrip('\n') == '1':
            entryMatches = userAgent.startswith(entry[0].lower())
        else:
            entryMatches = entry[0].lower() in userAgent
    return entryMatches


def matchBot(userAgent, entry):
    entryMatches = False
    debug = False
    # second field must be 1 for active matching
    if entry[1] == '1':
        # sixth field == 1 indicates match beginning of string
        if entry[5].rstrip('\n') == '1':
            entryMatches = userAgent.startswith(entry[0].lower())
        else:
            entryMatches = entry[0].lower() in userAgent

        # Handle exceptions
        if entryMatches and entry[2] != '':
            exceptions = entry[2].split(',')
            for exception in exceptions:
                if exception.lower() in userAgent:
                    entryMatches = False
                    break
    return entryMatches

def is_abf_bot(row):
    if ('api' != row['eventType'] and ABF_BOTS_MAP.has_key(row['ipAddress']) and row['userAgentStr'] in ABF_BOTS_MAP[row['ipAddress']]):
        return True
    else:
        return False


def load_abf_bot_map(bot_files_location):
    global ABF_BOTS_MAP
    abfbots_file = open(bot_files_location, 'r')
    for line in abfbots_file:
        (key, val) = [x.strip() for x in line.split('\t')]
        if (not ABF_BOTS_MAP.has_key(key)):
            ABF_BOTS_MAP[key] = []
        ABF_BOTS_MAP[key].append(val)

    print 'loaded %d bots from abf bots file' % len(ABF_BOTS_MAP)


def get_score(scores_string, score_code):
    scores_dict = dict((k.strip(), float(v.strip())) for k, v in (p.split('=') for p in scores_string[1:-1].split(',')))
    if scores_dict.has_key(score_code):
        score = float(scores_dict[score_code])
    else:
        score = None
    return score


def is_failed_impression(row):
    action = row['action']
    return action == 'failed' and row['isBot'] == False and row['isAbfBot'] == False and row['isValidBrowser'] == True


def failed_to_block_impression(row):
    action = row['action']
    override = row['override']
    visibility_score = get_score(row['scores'], 'visibility')
    return (action == 'passed' or action == 'preview') and override == 1 and visibility_score >= 1


def is_invisible_impression(row):
    scores = row['scores']
    visibility_score = get_score(scores, 'visibility')
    return (visibility_score is not None and visibility_score < 1) and row['isBot'] == False and row['isAbfBot'] == False and row['isValidBrowser'] == True


def process_data_frame_for_firewall_blocking_report(df_event_logs, valid_browser_list, bot_list):
    event_log_chunks = []
    for event_log_chunk in df_event_logs:
        # Normalize the dates to make them easier to aggregate
        event_log_chunk['date'] = event_log_chunk.dateReceived.apply(get_normalized_date)

        df_eventlogs_matched_user_agents = validateUserAgentList(event_log_chunk.userAgentStr.unique(), valid_browser_list, bot_list)

        # Merge both DataFrames using userAgentStr. Adds columns isValidBrowser and isBot
        event_log_chunk = event_log_chunk.merge(df_eventlogs_matched_user_agents, on='userAgentStr')

        # Tag is abf bot
        event_log_chunk['isAbfBot'] = event_log_chunk.apply(is_abf_bot, axis=1)

        # Tag passed impression
        event_log_chunk['isPassed'] = event_log_chunk.apply(is_passed_impression, axis=1)

        # Tag failed impression
        event_log_chunk['isFailed'] = event_log_chunk.apply(is_failed_impression, axis=1)

        # Tag failed by suspicious activity
        event_log_chunk['failed_by_suspicious'] = np.logical_and(event_log_chunk['isFailed'], event_log_chunk['reason'] == 'sad')

        # Tag failed by geography
        event_log_chunk['failed_by_geography'] = np.logical_and(event_log_chunk['isFailed'], np.isin(event_log_chunk['reason'], ['country', 'state', 'dma']))

        # Tag failed by adware
        event_log_chunk['failed_by_adware'] = np.logical_and(event_log_chunk['isFailed'], event_log_chunk['reason'] == 'adware')

        # Tag failed by keyword
        event_log_chunk['failed_by_keyword'] = np.logical_and(event_log_chunk['isFailed'], event_log_chunk['reason'] == 'keyword')

        # Tag failed by content
        event_log_chunk['failed_by_content'] = np.logical_and(event_log_chunk['isFailed'], np.isin(event_log_chunk['reason'], ['arbitration', 'lang']))

        # Tag failed by list
        event_log_chunk['failed_by_list'] = np.logical_and(event_log_chunk['isFailed'], np.isin(event_log_chunk['reason'], ['url', 'forbid', 'require']))

        # Tag should have blocked impression
        event_log_chunk['failed_to_block'] = event_log_chunk.apply(failed_to_block_impression, axis=1)

        # Tag invisible imps
        event_log_chunk['invisible_imps'] = event_log_chunk.apply(is_invisible_impression, axis=1)

        # Blocked See through - blocked by visibility impression
        event_log_chunk['blocked_see_through'] = np.logical_and(event_log_chunk['isFailed'], event_log_chunk['reason'] == 'visibility')

        event_log_chunks.append(event_log_chunk)

    return event_log_chunks


def percent(num1, num2):
    num1 = float(num1)
    num2 = float(num2)
    percentage = '{0:.2f}%'.format((num1 / num2 * 100))
    return percentage


def compose_firewall_blocking_report(processed_event_logs):

    df_aggregates = processed_event_logs[['isPassed',
                                   'isFailed',
                                   'failed_by_suspicious',
                                   'failed_by_geography',
                                   'failed_by_adware',
                                   'failed_by_keyword',
                                   'failed_by_content',
                                   'failed_by_list',
                                   'failed_to_block',
                                   'blocked_see_through',
                                   'invisible_imps',
                                   'isBot',
                                   'isValidBrowser',
                                   'isAbfBot']].sum()

    firewall_blocking_report = pd.concat([df_aggregates])

    # final aggregated firewall blocking report
    valid_browser_impressions = df_aggregates['isValidBrowser']
    bot_impressions = df_aggregates['isBot']
    abf_bot_impressions = df_aggregates['isAbfBot']
    total_impressions = valid_browser_impressions - bot_impressions - abf_bot_impressions
    firewall_blocking_report['total_impressions'] = total_impressions

    passed_impressions = df_aggregates['isPassed']
    passed_impressions_perc = percent(passed_impressions, total_impressions)
    firewall_blocking_report['passed_imps_perc'] = passed_impressions_perc

    failed_impressions = df_aggregates['isFailed']
    failed_imps_perc = percent(failed_impressions, total_impressions)
    firewall_blocking_report['failed_imps_perc'] = failed_imps_perc

    failed_to_block_impressions = df_aggregates['failed_to_block']
    failed_to_block_perc = percent(failed_to_block_impressions, total_impressions)
    firewall_blocking_report['failed_to_block_perc'] = failed_to_block_perc

    seen_through_impressions = total_impressions - processed_event_logs['invisible_imps'].sum()
    firewall_blocking_report['seen_through_impressions'] = seen_through_impressions
    seen_through_imps_perc = percent(seen_through_impressions, total_impressions)
    firewall_blocking_report['seen_through_imps_perc'] = seen_through_imps_perc

    print 'firewall blocking report json : %s' % firewall_blocking_report.to_json()

    # print firewall blocking report to csv
    firewall_blocking_report.to_csv('firewall_blocking_report.csv')
    print 'Saved Firewall Blocking report in firewall_blocking_report.csv'


def main():
    parser = setup_args_parser()
    args = vars(parser.parse_args())

    print 'args %s' % args

    load_abf_bot_map(args['abf_bots_file'])
    eventlog_input_file_location = args['event_log']

    valid_browser_file_location = args['valid_browsers_file']
    bot_file_path = args['iab_bots_file']

    # Get the user agents from the IAB lists
    valid_browser_list = initialize_iab_list(valid_browser_file_location)
    bot_list = initialize_iab_list(bot_file_path)

    df_event_logs = read_file(eventlog_input_file_location, event_log_columns)
    processed_event_log_chunk = process_data_frame_for_firewall_blocking_report(df_event_logs, valid_browser_list, bot_list)
    processed_event_logs = pd.concat(processed_event_log_chunk, axis=0)
    processed_event_logs.to_csv('processed_event_logs.tsv', sep='\t', encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)

    compose_firewall_blocking_report(processed_event_logs)


if __name__ == '__main__':
    main()
