// --------------------------------------------------------------------------------------------------------------------
// <copyright file="MediaEventsAggregation.cs" company="Sitecore A/S">
//   Copyright (C) 2015 by Sitecore A/S
// </copyright>
// <summary>
//   Defines the MediaEventsAggregation type.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Sitecore.MediaFramework.Pipelines.AnalyticsAggregation.Interactions
{
  using System;
  using System.Collections.Generic;
  using System.Linq;

  using Sitecore.Analytics.Aggregation.Pipeline;
  using Sitecore.Analytics.Core;
  using Sitecore.Analytics.Model;
  using Sitecore.ExperienceAnalytics.Api.Encoding;
  using Sitecore.MediaFramework.Data.Analytics;

  public class MediaEventsAggregation : AggregationProcessor
  {
    protected override void OnProcess(AggregationPipelineArgs args)
    {
      var dimension = args.GetDimension<MediaFrameworkMedia>();
      var fact = args.GetFact<MediaFrameworkEvents>();

      VisitData visit = args.Context.Visit;

      DateTime date = args.DateTimeStrategy.Translate(visit.StartDateTime);

      foreach (PageEventData pageEvent in this.GetPageEvents(visit))
      {
        var mediaEvent = MediaEventData.Parse(pageEvent);
        if (mediaEvent == null)
        {
          continue;
        }

        Hash128 mediaId = dimension.AddValue(mediaEvent);
        Sites.Site site = Sites.SiteManager.GetSite(visit.SiteName);
        var encoder = new Hash32Encoder();
        string hashName = encoder.Encode(visit.SiteName);
        
        var key = new MediaFrameworkEventsKey
          {
            Date = date,
            MediaId = mediaId,
            PageEventDefinitionId = pageEvent.PageEventDefinitionId,
            EventParameter = mediaEvent.EventParameter,
            SiteNameId = int.Parse(hashName)
          };

        var value = new MediaFrameworkEventsValue { Count = 1 };

        fact.Emit(key, value);
      }
    }

    protected virtual List<PageEventData> GetPageEvents(VisitData visit)
    {
      return new List<PageEventData>(
        visit.Pages
          .Where(page => page.PageEvents != null)
          .SelectMany(page => page.PageEvents)
          .Where(pageEvent => MediaFrameworkContext.IsMediaEvent(pageEvent.PageEventDefinitionId)));
    }
  }
}