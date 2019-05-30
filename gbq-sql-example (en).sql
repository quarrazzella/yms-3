-- the query combines online and offline data using siteOrderId (a unique ID for a credit loan application)
-- it is used to distribute revenue from disbursed loans between different marketing channels
-- thus, we can study marketing channels not only in the context of the quantity of gained applications,
-- but also from the standpoint of the revenue acquired from this loans.

SELECT
  -- resulting columns
  main_table.date,
  channel,
  region,
  main_table.source,
  main_table.medium,
  totalLeads,
  totalOrders,
  IF(totalOrders !=0, ROUND(totalOrders / totalLeads * 100 ,2), 0) AS orderRate,
  totalProcessed,
  IF(totalCredits !=0, ROUND(totalProcessed / totalOrders * 100 ,2), 0) AS processRate,
  totalApproved,
  IF(totalCredits !=0, ROUND(totalApproved / totalProcessed * 100 ,2), 0) AS approveRate,
  totalCredits,
  IF(totalCredits !=0, ROUND(totalCredits / totalOrders * 100 ,2), 0) AS takeRate,
  totalCreditsSum
FROM(
	-- gathering data by: date, source, region
SELECT
  date,
  channel,
  region,
  source,
  medium,
  COUNT(*) AS totalLeads,
  SUM(IF(name IS NOT NULL, 1,0)) AS totalOrders,
  SUM(IF(REGEXP_CONTAINS(name, 'На выдачу|Выдан кредит|Договор подписан'), 1,0)) totalCredits,
  SUM(IF(name != 'Введена' AND name != 'Корректировка' AND name != 'В обработке', 1,0)) totalProcessed,
  SUM(IF(REGEXP_CONTAINS(name, 'Отозвана|На выдачу|Утверждена|Выдан кредит|Договор открыт|Не востребована|Договор подписан|Условно утверждена|Формирование договора'), 1,0)) totalApproved,
  FORMAT('%.2f', SUM(IF(REGEXP_CONTAINS(name, 'На выдачу|Выдан кредит|Договор подписан'), CAST(revenue AS NUMERIC), NULL))) totalCreditsSum
FROM(
  SELECT
      site_leads.date,
      site_leads.region,
      site_leads.siteOrderId,
      site_leads.source,
      site_leads.medium,
      -- a custom channel grouping (marketing team request)
      CASE
        WHEN REGEXP_CONTAINS((CONCAT(site_leads.source, ' / ', site_leads.medium)), 'organic') THEN 'organic'
        WHEN REGEXP_CONTAINS((CONCAT(site_leads.source, ' / ', site_leads.medium)), 'vbr_ru|banki_ru|sravni_ru|leadgid_ru') THEN 'CPA'
        WHEN REGEXP_CONTAINS((CONCAT(site_leads.source, ' / ', site_leads.medium)), 'google / cpc') THEN 'google'
        WHEN REGEXP_CONTAINS((CONCAT(site_leads.source, ' / ', site_leads.medium)), r'yandex_.* / cpc') THEN 'yandex'
        WHEN REGEXP_CONTAINS((CONCAT(site_leads.source, ' / ', site_leads.medium)), r'yandex\.ru \/ referral') THEN 'organic'
        WHEN REGEXP_CONTAINS((CONCAT(site_leads.source, ' / ', site_leads.medium)), r'\(direct\) \/ \(none\)') THEN 'direct_entrance'
        WHEN REGEXP_CONTAINS((CONCAT(site_leads.source, ' / ', site_leads.medium)), 'referral') THEN 'referral'
        WHEN REGEXP_CONTAINS((CONCAT(site_leads.source, ' / ', site_leads.medium)), 'drks_.* / sms') THEN 'SMS'
        WHEN REGEXP_CONTAINS((CONCAT(site_leads.source, ' / ', site_leads.medium)), 'drks_.* / email') THEN 'EMAIL'
        WHEN REGEXP_CONTAINS((CONCAT(site_leads.source, ' / ', site_leads.medium)), 'facebook|instagram_com|mytarget_com|vk_com') THEN 'SMM'
        ELSE 'other'
      END AS channel,
      site_orders.name,
      site_orders.revenue
  FROM(
  	-- here are data for online applications
    SELECT
      date,
      IF(REGEXP_CONTAINS(hits.eventInfo.eventCategory, r"^потребительский кредит \(длинная заявка\)|^$")
        OR hits.eventInfo.eventAction = "отправка заявки",
        CONCAT("NP", SPLIT(hits.eventInfo.eventLabel, ' - ')[OFFSET(1)]),
        CONCAT("UP", SPLIT(hits.eventInfo.eventLabel, ' - ')[OFFSET(1)])) siteOrderId,
      trafficSource.source,
      trafficSource.medium,
      geoNetwork.region
    FROM
      `akbarsgadata-214816.akbars_ru_only.owoxbi_sessions_2019*`,
      UNNEST(hits) AS hits
    WHERE
      REGEXP_CONTAINS(hits.eventInfo.eventCategory, r"^потребительский кредит|^потребительский кредит \(длинная заявка\)|^$")
      AND REGEXP_CONTAINS(hits.eventInfo.eventAction, r"личные данные|личные и паспортные данные|^$|отправка заявки")
      AND REGEXP_CONTAINS(hits.eventInfo.eventLabel, r"^Отправка заявки - ")
      AND _TABLE_SUFFIX BETWEEN '0101' AND '0409' AND _TABLE_SUFFIX != '0401'
      AND hits.page.hostname = "www.akbars.ru"
      UNION ALL
      SELECT date, siteOrderId, SPLIT(source_medium, ' / ')[OFFSET(0)] utm_source, SPLIT(source_medium, ' / ')[OFFSET(1)] utm_medium, region FROM `akbarsgadata-214816.test_akbars_ru.consume_lost_leads_region`
    ) AS site_leads
  -- and here are data for offline applications' processing
  LEFT JOIN(
    SELECT
      CAST(EXTRACT(DATE FROM time) AS string) date,
      siteOrderId,
      name,
      revenue
    FROM
      `akbarsgadata-214816.test_akbars_ru.consume_okz_0410`
    WHERE
      siteOrderId IS NOT NULL
  ) AS site_orders ON site_leads.siteOrderId = site_orders.siteOrderId
)
GROUP BY channel, source, medium, region, date
ORDER BY totalLeads DESC) AS main_table
ORDER BY Date