'''
rmq_send.py

@version: 1.0

Created on May, 17, 2015

@author: Glenn Kroegel
@contact: glenn.kroegel@gmail.com
@summary: Receives live data from API. Data is sent to rmq_receive.py where it is used in ML model.

'''
using System;
using System.Text;
using System.Threading;
using RabbitMQ.Client;
using System.Linq;
using cAlgo.API;
using cAlgo.API.Indicators;
using cAlgo.API.Internals;
using cAlgo.Indicators;

namespace cAlgo
{
    [Robot(TimeZone = TimeZones.UTC, AccessRights = AccessRights.FullAccess)]
    public class MQ5 : Robot
    {
        [Parameter(DefaultValue = 0.0)]
        public double Parameter { get; set; }

        private double open;
        private double high;
        private double low;
        private double close;
        private double volume;
        private int lastIndex;
        private int index;
        private int window = 300;
        private string sym;
        private DateTime time;
        private string current;
        private string header = string.Join(",", "DATETIME", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME");
        private StringBuilder export = new StringBuilder();

        protected override void OnStart()
        {
            // Put your initialization logic here

            sym = Symbol.Code;
            Print("{0}", sym);

            // Gather initial statistics
            export.Clear();
            export.AppendLine(header);

            for (int i = window; i >= 2; i--)
            {

                index = MarketSeries.Close.Count - i;
                time = MarketSeries.OpenTime[index];
                open = MarketSeries.Open[index];
                high = MarketSeries.High[index];
                low = MarketSeries.Low[index];
                close = MarketSeries.Close[index];
                volume = MarketSeries.TickVolume[index];

                current = string.Join(",", time.ToString(), open.ToString("n5"), high.ToString("n5"), low.ToString("n5"), close.ToString("n5"), volume.ToString("n5"));
                export.AppendLine(current);
            }
        }


        protected override void OnBar()
        {

            // Append latest tick

            time = MarketSeries.OpenTime.Last(1);
            open = MarketSeries.Open.Last(1);
            high = MarketSeries.High.Last(1);
            low = MarketSeries.Low.Last(1);
            close = MarketSeries.Close.Last(1);
            volume = MarketSeries.TickVolume.Last(1);

            current = string.Join(",", time.ToString("yyyy-MM-dd H:mm:ss"), open.ToString("n5"), high.ToString("n5"), low.ToString("n5"), close.ToString("n5"), volume.ToString("n5"));
            export.AppendLine(current);

            // Message Queue

            var factory = new ConnectionFactory 
            {
                HostName = "localhost"
            };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    sym = Symbol.Code;
                    var body = Encoding.UTF8.GetBytes(export.ToString());
                    var properties = channel.CreateBasicProperties();
                    channel.BasicPublish("", sym, properties, body);
                }
            }

        }

        protected override void OnStop()
        {

        }
    }
}
