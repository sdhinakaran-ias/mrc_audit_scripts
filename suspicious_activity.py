import pandas as pd
import argparse
import csv

SITTING_DUCKS_BOTS = 1
STANDARD_BOTS = 2
VOLUNTEER_BOTS = 3
PROFILE_BOTS = 4
MASKED_BOTS = 5
NOMADIC_BOTS = 6
OTHER_BOTS = 7

eventLogColumns = [
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
    'setCustomMetricIds',
    'dealId',
    'selectedPartnerName',
    'selectedPartnerVersion',
    'selectedPartnerSource', 
    'os',
    'osVersion',
    'browser',
    'browserVersion',
    'platformName',
    'extAdSessionId',
    'extMediaType',
    'extAdSessionId',
    'extMediaType',
    'extIsMobileApp'
]

qlogColumns = [
    'version',
    'dateReceived',
    'timeReceived',
    'timeRange',
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
    'eventCount',
    'adtalkTimestamp',
    'adtalkCall',
    'flashTimestamp',
    'flashCall',
    'unloadTimestamp',
    'unloadCall',
    'pings',
    'dtCount',
    'impressionScores',
    'impressionScoreWeights',
    'auxInfo',
    'ext_bidUr',
    'ext_bidPrice',
    'ext_uid',
    'ext_impId',
    'sadEvidence',
    'exchangeInfo',
    'macros',
    'cookieValue',
    'diagnosticsField',
    'videoTimestamp',
    'videoCall',
    'originalReferringUrl',
    'mbTimestamp',
    'mbCall',
    'scaChallenges',
    'hundredPercentPings',
    'videoQuartileHundredPercentNotMutedPings',
    'originalIPAddress',
    'asIdMap',
    'pubOrder',
    'pubCreative',
    'custom',
    'custom2',
    'custom3',
    'oneTimePings',
    'bundleId',
    'sdkAppId',
    'givt',
    'viewabilityMethod',
    'scaResults',
    'discFree',
    'discFreePassbackId',
    'discFreeAction',
    'dtMinimizer',
    'discFreeReason',
    'appStoreTypeCode',
    'selectedBundleId',
    'selectedBundleIdSource',
    'identifiedBundleIds',
    'cmCLogTimestamp',
    'cmCLog',
    'cmSubIds',
    'fraudScores',
    'cmIds',
    'mrcOutofViewReason',
    'auth',
    'gdprSanitizedIpAddress',
    'dealId',
    'override',
    'discFreeReasonElement',
    'os',
    'osVersion',
    'browser',
    'browserVersion',
    'platformName'
]

ABFBOTS_MAP = {}

def read_file(input_file_location, columns):
    df = pd.read_table(input_file_location,
                       sep='\t',
                       header=None,
                       names=columns,
                       low_memory=False,
                       chunksize=100000, ## only read 100,000 rows at a time.
                       infer_datetime_format=True,
                       parse_dates=['dateReceived'])
    print 'loading file %s' % input_file_location
    return df

def initializeIABList(iabFilePath):
    iabList = []

    iabFile = open(iabFilePath, 'r')
    for line in iabFile:
        if not (line.startswith('#')):
            tokens = line.split('|')
            iabList.append(tokens)

    iabFile.close()
    return iabList

def initializeMonitoredEntities(pubEntityFilePath):
    monitoredEntities = [] 
    
    pubEntityFile = open(pubEntityFilePath, 'r')
    for line in pubEntityFile:
        monitoredEntities.append(int(line.strip()))    

    pubEntityFile.close()
    print 'loaded %d pub entities from pub entity list  file' % len(monitoredEntities)
    return monitoredEntities


def getNormalizedDate(impressionDate):
    return pd.datetime(impressionDate.year, impressionDate.month, impressionDate.day)

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

def isPassedImpression(action):
    return action == 'passed' or action == 'preview'

def isInView(row):
    if (row['isVideo']):
        return 'iv2=1.0' in row['impressionScores']
    else:
        return 'iv1=1.0' in row['impressionScores']

def matchMonitoredList(passbackIds, monitoredEntities):
    matchedPubIds = []

    for passbackId in passbackIds:
        isMonitored = passbackId in monitoredEntities or passbackId == -1
        matchedPubIds.append({'passbackId': passbackId, 'isMonitored': isMonitored})
    return pd.DataFrame(matchedPubIds)

def not_abf_bot(row):
    if ('api' != row['eventType'] and ABFBOTS_MAP.has_key(row['ipAddress']) and row['userAgentStr'] in ABFBOTS_MAP[row['ipAddress']]):
        return False
    else:
        return True

def load_abf_bot_map(bot_files_location):
    global ABFBOTS_MAP
    abfbots_file = open(bot_files_location, 'r')
    for line in abfbots_file:
        (key, val) = [x.strip() for x in line.split('\t')]
        if (not ABFBOTS_MAP.has_key(key)):
            ABFBOTS_MAP[key] = []
        ABFBOTS_MAP[key].append(val)

    print 'loaded %d bots from abf bots file' % len(ABFBOTS_MAP)

def is_suspicious_impression(row):
    if (int(row['givt']) == 0 and 'rsa=0' in row['scores']):
        return True
    else:
        return False

def is_desktop(platform):
    return platform not in ['null', 'mob', 'tab']

def find_env_str(javascriptInfo):
    return 'APP' if ('mapp' in javascriptInfo) else 'BROWSER'

def find_platform_str(isDesktop, platform):
    if (isDesktop == True):
        return 'DESKTOP'
    elif (platform == 'mob'):
       return 'PHONE'
    elif (platform == 'tab'):
       return 'TABLET'

    return 'UNKNOWN'

def find_media_format_str(isVideo):
    return 'VIDEO' if isVideo == True else 'DISPLAY'

def find_media_type(row):
    env_str = find_env_str(row['javascriptInfo'])
    platform_str = find_platform_str(row['isDesktop'], row['platform'])
    media_format_str = find_media_format_str(row['isVideo'])

    return "{0}_{1}_{2}".format(env_str, platform_str, media_format_str)

def compose_susp_activity_report(dfMergedQlog, dfMergedEventLog):
    dfBotsAgg = dfMergedQlog[['sittingDucks',
                              'standardBots',
                              'volunteerBots',
                              'profileBots',
                              'maskedBots',
                              'nomadicBots',
                              'otherBots'
                              ]].sum()

    # total impressions is calculated from event logs
    dfTotalImpressions = dfMergedEventLog[['isValid']].sum()
    totalImpressions = dfTotalImpressions['isValid']

    # suspicious impressions is calculated from quality logs
    dfSuspiciousImpressions = dfMergedQlog[['isSuspicious']].sum()
    suspiciousImps = dfSuspiciousImpressions['isSuspicious']
    nonSuspiciousImps = totalImpressions - suspiciousImps
    dfSuspActivityReport = pd.concat([dfBotsAgg])
    dfSuspActivityReport['totalImpressions'] = totalImpressions
    dfSuspActivityReport['suspiciousImpressions'] = suspiciousImps
    dfSuspActivityReport['nonSuspiciousImps'] = nonSuspiciousImps
    suspImpsPerc = percent(suspiciousImps, totalImpressions)
    dfSuspActivityReport['suspiciousImpsPerc'] = suspImpsPerc
    nonSuspImpsPerc = percent(nonSuspiciousImps, totalImpressions)
    dfSuspActivityReport['nonSuspiciousImpsPerc'] = nonSuspImpsPerc

    print 'susp json : %s' %dfSuspActivityReport.to_json()

    dfSuspActivityReport.to_csv('suspicious_activity.csv')
    print 'Suspicious Activity report is stored in suspicious_activity.csv'

def percent(num1, num2):
    num1 = float(num1)
    num2 = float(num2)
    percentage = '{0:.2f}%'.format((num1 / num2 * 100))
    return percentage

def process_event_logs(dfEventLogs, validBrowserList, botList, monitoredEntities):
    # Iterating through event log chunks
    eventlog_chunks = []
    for eventlog_chunk in dfEventLogs:
        eventlog_chunk = process_base_log_chunks(eventlog_chunk, validBrowserList, botList, monitoredEntities)

        eventlog_chunks.append(eventlog_chunk)

    return eventlog_chunks


def process_qlogs(dfQlogs, validBrowserList, botList, monitoredEntities):
    qlog_chunks = []
    for qlog_chunk in dfQlogs:

        qlog_chunk = process_base_log_chunks(qlog_chunk, validBrowserList, botList, monitoredEntities)
        # Tag isVideo call
        qlog_chunk['isVideo'] = qlog_chunk.javascriptInfo.apply(lambda x: 'jsvid' in x)

       	# Tag isInView
	qlog_chunk['isInView'] = qlog_chunk.apply(isInView, axis=1)

        # Tag isMeasured
        qlog_chunk['isMeasured'] = qlog_chunk.impressionScoreWeights.apply(lambda x: ' smp=0.0' not in x)

        # Tag isGrossMeasured
        qlog_chunk['isGrossMeasured'] = qlog_chunk.impressionScoreWeights.apply(lambda x: 'vtrust=1.0' in x)

        # Tag isDesktop
        qlog_chunk['isDesktop'] = qlog_chunk.platform.apply(is_desktop)

	# Tag mediaType
	qlog_chunk['mediaType'] = qlog_chunk.apply(find_media_type, axis=1)

        # Tag scaling factor
        qlog_chunk['scalingFactor'] = 1.0

        # Tag suspicious impressions
        qlog_chunk['isSuspicious'] = qlog_chunk.apply(is_suspicious_impression, axis=1)

        # Tag Sitting Duck
        qlog_chunk['sittingDucks'] = qlog_chunk.fraudScores.apply(lambda x: 'nht1=1.0' in x)

        # Tag Standard Bots
        qlog_chunk['standardBots'] = qlog_chunk.fraudScores.apply(lambda x: 'nht2=1.0' in x)

        # Tag Volunteer Bots
        qlog_chunk['volunteerBots'] = qlog_chunk.fraudScores.apply(lambda x: 'nht3=1.0' in x)

        # Tag Profile Bots
        qlog_chunk['profileBots'] = qlog_chunk.fraudScores.apply(lambda x: 'nht4=1.0' in x)

        # Tag Masked Ducks
        qlog_chunk['maskedBots'] = qlog_chunk.fraudScores.apply(lambda x: 'nht5=1.0' in x)

        # Tag Nomadic Ducks
        qlog_chunk['nomadicBots'] = qlog_chunk.fraudScores.apply(lambda x: 'nht6=1.0' in x)

        # Tag Other bots
        qlog_chunk['otherBots'] = qlog_chunk.fraudScores.apply(lambda x: 'nht7=1.0' in x)

        qlog_chunks.append(qlog_chunk)
    return qlog_chunks


def process_base_log_chunks(chunk, validBrowserList, botList, monitoredEntities):
    # Normalize the dates to make them easier to aggregate
    chunk['date'] = chunk.dateReceived.apply(getNormalizedDate)

    # Get the list of user agent string for IAB filtering and put them in a DataFrame. We pass unique userAgentStr for efficiency and merge them back later
    dfQlogsMatchedUserAgents = validateUserAgentList(chunk.userAgentStr.unique(), validBrowserList, botList)

    # Merge both DataFrames using userAgentStr. Adds columns isValidBrowser and isBot
    chunk = chunk.merge(dfQlogsMatchedUserAgents, on='userAgentStr')

    # Get the list of unique monitored impressions. We pass unique passbackId for efficiency and merge them back later
    dfMonitoredEntities = matchMonitoredList(chunk.passbackId.unique(), monitoredEntities)

    # Merge both DataFrames using passbackId. Adds column isMonitored
    chunk = chunk.merge(dfMonitoredEntities, on='passbackId')

    # Tag passed impressions
    chunk['isPassed'] = chunk.action.apply(isPassedImpression)

    # Tag valid impressions that are both a valid browser and not a bot
    chunk['isValid'] = chunk.isValidBrowser & ~chunk.isBot & chunk.apply(not_abf_bot, axis=1)

    # Tag blocked impressions
    chunk['isBlocked'] = (chunk.isMonitored == False) & ~chunk.isPassed

    # Tag suspicious impressions
    chunk['isSuspicious'] = chunk.scores.apply(lambda x: 'rsa=0' in x)

    return chunk

def computeGrossAndNetMetrics(dfEventLogs, dfQualityLogs):
    # Gross Metrics
    dfGrossMetrics = pd.DataFrame()

    # Get the total gross number of unblocked impressions per day
    unblockedEventLogsImpressionsPerDay = dfEventLogs[~dfEventLogs.isBlocked & dfEventLogs.isValid].groupby(['date']).action.count()
    
    # We can't fill in this column yet because we need invalid impressions that are computed later
    dfGrossMetrics['totalUnblockedImpressions'] = 0

    # Measured impressions = gross measured impressions that are not blocked
    measuredImpressionsPerDay = dfQualityLogs[dfQualityLogs.isGrossMeasured & ~dfQualityLogs.isBlocked].groupby(['date']).action.count()
    dfGrossMetrics['measuredImpressions'] = measuredImpressionsPerDay

    # Suspicious Impressions = Impressions that are not blocked AND are valid AND are suspicious
    suspiciousImpressionsPerDay = dfEventLogs[~dfEventLogs.isBlocked & dfEventLogs.isValid & dfEventLogs.isSuspicious].groupby(['date']).action.count()

    # Get all impressions that are invalid
    invalidImpressionsPerDay = dfQualityLogs[~dfQualityLogs.isValid].groupby(['date']).isValid.count() 

    # Perform a left join with the main dfGrossMetrics table. 
    # We reset the index and then recreate it to preserve the date column, fill NaN values with 0 and convert type of the Series to int
    invalidImpressionsPerDay = dfGrossMetrics.reset_index().merge(pd.DataFrame(invalidImpressionsPerDay).reset_index(), how='left').set_index('date').isValid.fillna(0).astype(int)

    # Now we can compute Total Unblocked and Gross Unblocked Invalid Impressions
    dfGrossMetrics['totalUnblockedImpressions'] = unblockedEventLogsImpressionsPerDay + invalidImpressionsPerDay
    dfGrossMetrics['grossUnblockedInvalidImpressions'] = suspiciousImpressionsPerDay + invalidImpressionsPerDay

    # Net Metrics

    dfNetMetrics = pd.DataFrame()

    # Total Eligible Impressions = Impressions that are not Blocked AND are valid
    dfNetMetrics['totalEligibleImpressions'] = dfEventLogs[~dfEventLogs.isBlocked & dfEventLogs.isValid].groupby(['date']).isValid.count()

    # Temporarily store In-View and Out-Of-View to preserve column order so that it's consistent with the UI
    # Net Viewable Impressions = Impressions that are in-view AND are gross measured AND are not blocked
    netViewableImpressesionPerDay = dfQualityLogs[dfQualityLogs.isInView & dfQualityLogs.isGrossMeasured & ~dfQualityLogs.isBlocked].groupby(['date']).isInView.count()
    netNonViewableImpressionsPerDay = dfQualityLogs[~dfQualityLogs.isInView & dfQualityLogs.isGrossMeasured & ~dfQualityLogs.isBlocked].groupby(['date']).isInView.count() 

    dfNetMetrics['measuredImpressions'] = netViewableImpressesionPerDay + netNonViewableImpressionsPerDay
    dfNetMetrics['viewableImpressions'] = netViewableImpressesionPerDay
    dfNetMetrics['nonViewable'] = netNonViewableImpressionsPerDay 
    dfNetMetrics['unmeasuredImpressions'] = dfNetMetrics.totalEligibleImpressions - dfNetMetrics.measuredImpressions

    # Total Net Metrics

    dfTotalNetMetrics = pd.DataFrame() 

    # Total Eligible impressions = Impressions that are not Blocked AND valid AND are not suspicious
    dfTotalNetMetrics['totalEligibleImpressions'] = dfEventLogs[~dfEventLogs.isBlocked & dfEventLogs.isValid & ~dfEventLogs.isSuspicious].groupby(['date']).isValid.count()

    # measured impressions = Impressions that are measured AND not blocked AND not suspicious
    dfTotalNetMetrics['measuredImpressions'] = dfQualityLogs[dfQualityLogs.isMeasured & ~dfQualityLogs.isBlocked & ~dfQualityLogs.isSuspicious].groupby(['date']).action.count()

    # In-view impressions = Impressions that are in-view AND are measured AND are not blocked AND are not suspicious
    dfTotalNetMetrics['inViewImpressions'] = dfQualityLogs[dfQualityLogs.isInView & dfQualityLogs.isMeasured & ~dfQualityLogs.isBlocked & ~dfQualityLogs.isSuspicious].groupby(['date']).isInView.count() 

    # In-view impressions = Impressions that are not in-view AND are measured AND are not blocked AND are not suspicious
    dfTotalNetMetrics['outOfViewImpressions'] = dfQualityLogs[~dfQualityLogs.isInView & dfQualityLogs.isMeasured & ~dfQualityLogs.isBlocked & ~dfQualityLogs.isSuspicious].groupby(['date']).isInView.count() 

    dfTotalNetMetrics['unmeasuredImpressions'] = dfTotalNetMetrics.totalEligibleImpressions - dfTotalNetMetrics.measuredImpressions

    dfGrossMetrics.to_csv('gross_metrics.csv')
    dfNetMetrics.to_csv('net_metrics.csv')
    dfTotalNetMetrics.to_csv('total_net_metrics.csv')

def setup_args_parsers():
    parser = argparse.ArgumentParser()
    parser.add_argument('--event_log', required=True, help='location of event log file ')
    parser.add_argument('--qlog', required=True, help='location for qlog file')
    parser.add_argument('--valid_browsers_file', required=True,  help='valid browsers file')
    parser.add_argument('--iab_bots_file', required=True, help='iab bots file')
    parser.add_argument('--abf_bots_file', required=True, help='abf bots file location')
    parser.add_argument('--pub_entity_file', required=True, help='publisher entities that are monitored')
    return parser

def main():
    parser = setup_args_parsers()
    args = vars(parser.parse_args())
    print 'args %s' % args

    load_abf_bot_map(args['abf_bots_file'])

    eventlog_input_file_location = args['event_log']
    qlog_input_file_location = args['qlog']

    valid_browser_file_location = args['valid_browsers_file']
    bot_file_path = args['iab_bots_file']

    pub_entity_file_path = args['pub_entity_file']
 

    # Get the user agents from the IAB lists
    validBrowserList = initializeIABList(valid_browser_file_location)
    botList = initializeIABList(bot_file_path)
    
    # Get pub monitored entities
    monitoredEntities = initializeMonitoredEntities(pub_entity_file_path) 

    # DataFrame to load eventlogs
    dfEventLogs = read_file(eventlog_input_file_location, eventLogColumns)

    eventlog_chunks = process_event_logs(dfEventLogs, validBrowserList, botList, monitoredEntities)

    # Dataframe to load qlogs
    dfQlogs = read_file(qlog_input_file_location, qlogColumns)

    qlog_chunks = process_qlogs(dfQlogs, validBrowserList, botList, monitoredEntities)

    # Merging all log chunks
    dfMergedQlog = pd.concat(qlog_chunks, axis=0)
    dfMergedEventLog = pd.concat(eventlog_chunks, axis=0)
   
    computeGrossAndNetMetrics(dfMergedEventLog, dfMergedQlog)
 
    dfMergedQlog.to_csv('computed_qlog.tsv', sep='\t',encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
    dfMergedEventLog.to_csv('computed_eventlog.tsv',sep='\t',encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
    compose_susp_activity_report(dfMergedQlog, dfMergedEventLog)


if __name__ == '__main__':
    main()
