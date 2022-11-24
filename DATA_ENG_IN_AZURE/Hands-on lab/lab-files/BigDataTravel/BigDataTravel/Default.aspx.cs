using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.Threading.Tasks;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Collections.Specialized;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Runtime.Serialization;
using NodaTime;
using Unidevel.OpenWeather;
using System.Reflection;

namespace BigDataTravel
{
    public partial class _Default : Page
    {
        private List<Airport> airports = null;
        private ForecastResult forecast = null;
        private DelayPrediction prediction = null;
        private static IOpenWeatherClient openWeatherClient;

        // settings
        private string mlUrl;
        private string pat;
        private string weatherApiKey;

        protected void Page_Load(object sender, EventArgs e)
        {
            InitSettings();
            InitAirports();

            if (!IsPostBack)
            {
                txtDepartureDate.Text = DateTime.Now.AddDays(5).ToShortDateString();

                openWeatherClient = new OpenWeatherClient(apiKey: weatherApiKey);

                ddlOriginAirportCode.DataSource = airports;
                ddlOriginAirportCode.DataTextField = "AirportCode";
                ddlOriginAirportCode.DataValueField = "AirportCode";
                ddlOriginAirportCode.DataBind();

                ddlDestAirportCode.DataSource = airports;
                ddlDestAirportCode.DataTextField = "AirportCode";
                ddlDestAirportCode.DataValueField = "AirportCode";
                ddlDestAirportCode.DataBind();
                ddlDestAirportCode.SelectedIndex = 12;
            }
        }

        private void InitSettings()
        {
            mlUrl = System.Web.Configuration.WebConfigurationManager.AppSettings["mlUrl"];
            pat = System.Web.Configuration.WebConfigurationManager.AppSettings["pat"];
            weatherApiKey = System.Web.Configuration.WebConfigurationManager.AppSettings["weatherApiKey"];
        }

        private void InitAirports()
        {
            airports = new List<Airport>()
            {
                new Airport() { AirportCode ="SEA", Latitude = 47.44900, Longitude = -122.30899 },
                new Airport() { AirportCode ="ABQ", Latitude = 35.04019, Longitude = -106.60900 },
                new Airport() { AirportCode ="ANC", Latitude = 61.17440, Longitude = -149.99600 },
                new Airport() { AirportCode ="ATL", Latitude = 33.63669, Longitude = -84.42810 },
                new Airport() { AirportCode ="AUS", Latitude = 30.19449, Longitude = -97.66989 },
                new Airport() { AirportCode ="CLE", Latitude = 41.41170, Longitude = -81.84980 },
                new Airport() { AirportCode ="DTW", Latitude = 42.21239, Longitude = -83.35340 },
                new Airport() { AirportCode ="JAX", Latitude = 30.49410, Longitude = -81.68789 },
                new Airport() { AirportCode ="MEM", Latitude = 35.04240, Longitude = -89.97669 },
                new Airport() { AirportCode ="MIA", Latitude = 25.79319, Longitude = -80.29060 },
                new Airport() { AirportCode ="ORD", Latitude = 41.97859, Longitude = -87.90480 },
                new Airport() { AirportCode ="PHX", Latitude = 33.43429, Longitude = -112.01200 },
                new Airport() { AirportCode ="SAN", Latitude = 32.73360, Longitude = -117.19000 },
                new Airport() { AirportCode ="SFO", Latitude = 37.61899, Longitude = -122.37500 },
                new Airport() { AirportCode ="SJC", Latitude = 37.36259, Longitude = -121.92900 },
                new Airport() { AirportCode ="SLC", Latitude = 40.78839, Longitude = -111.97799 },
                new Airport() { AirportCode ="STL", Latitude = 38.74869, Longitude = -90.37000 },
                new Airport() { AirportCode ="TPA", Latitude = 27.97550, Longitude = -82.53320 }
            };
        }

        protected async void btnPredictDelays_Click(object sender, EventArgs e)
        {
            var departureDate = DateTime.Parse(txtDepartureDate.Text);
            departureDate = departureDate.AddHours(double.Parse(txtDepartureHour.Text));

            var selectedAirport = airports.FirstOrDefault(a => a.AirportCode == ddlOriginAirportCode.SelectedItem.Value);

            if (selectedAirport != null)
            {
                var query = new DepartureQuery()
                {
                    DepartureDate = departureDate,
                    DepartureDayOfWeek = ((int)departureDate.DayOfWeek) + 1, //Monday = 1
                    Carrier = txtCarrier.Text,
                    OriginAirportCode = selectedAirport.AirportCode,
                    OriginAirportLat = selectedAirport.Latitude,
                    OriginAirportLong = selectedAirport.Longitude,
                    DestAirportCode = ddlDestAirportCode.SelectedItem.Text
                };

                await GetWeatherForecast(query);

                if (forecast == null)
                    throw new Exception("Forecast request did not succeed. Check Settings for weatherApiKey.");

                await PredictDelays(query, forecast);
            }

            UpdateStatusDisplay(prediction, forecast);
        }

        private void UpdateStatusDisplay(DelayPrediction prediction, ForecastResult forecast)
        {
            weatherForecast.ImageUrl = forecast.ForecastIconUrl;
            weatherForecast.ToolTip = forecast.Condition;

            if (String.IsNullOrWhiteSpace(mlUrl) || String.IsNullOrWhiteSpace(pat))
            {
                lblPrediction.Text = "(not configured)";
                return;
            }

            if (prediction == null)
                throw new Exception("Prediction did not succeed. Check the Settings for mlUrl and pat.");

            if (prediction.ExpectDelays)
            {
                lblPrediction.Text = "expect delays";
            }
            else
            {
                lblPrediction.Text = "no delays expected";
            }
        }

        private async Task GetWeatherForecast(DepartureQuery departureQuery)
        {
            var departureDate = departureQuery.DepartureDate;
            forecast = null;

            try
            {
                var weatherPrediction = await openWeatherClient.GetWeatherForecast5d3hAsync(
                    (float)departureQuery.OriginAirportLong,
                    (float)departureQuery.OriginAirportLat);
                if (weatherPrediction != null && weatherPrediction.List.Any())
                {
                    // Extract the dates from the prediction, then find the closest matching date and time based on the departure date.
                    var dates = weatherPrediction.List.Select(x => x.DateTimeUtc).ToList();
                    var nearestDiff = dates.Min(date => Math.Abs((date - departureDate).Ticks));
                    var nearestDate = dates.First(date => Math.Abs((date - departureDate).Ticks) == nearestDiff);
                    forecast = (from f in weatherPrediction.List
                        where f.DateTimeUtc == nearestDate
                        select new ForecastResult()
                        {
                            WindSpeed = f.Wind?.SpeedKmph ?? 0,
                            Precipitation = f.Rain?.OneHourMm ?? 0,
                            Pressure = f.Main?.PressurehPa*0.029529983071445 ?? 0, //Conversion from inHg to kPa
                            ForecastIconUrl = GetImagePathFromIcon(f.Weather[0].Icon),
                            Condition = f.Weather[0].Description
                        }).FirstOrDefault();
                }

            }
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError("Failed retrieving weather forecast: " + ex.ToString());
            }
        }

        private string GetImagePathFromIcon<T>(T value)
            where T : struct, IConvertible
        {
            var defaultIconPath = Page.ResolveUrl("~/images/cloudy.svg");
            if (value.ToString() == "None")
            {
                return defaultIconPath;
            }
            var enumType = typeof(T);
            var memInfo = enumType.GetMember(value.ToString());
            var attr = memInfo.FirstOrDefault()?.GetCustomAttributes(false).OfType<EnumMemberAttribute>().FirstOrDefault();
            return attr != null ? Page.ResolveUrl($"~/images/{attr.Value}.svg") : defaultIconPath;
        }

        private string GetImagePathFromIcon(string value)
        {
            var defaultIconPath = "http://openweathermap.org/img/wn/02d@2x.png";
            if (string.IsNullOrEmpty(value))
            {
                return defaultIconPath;
            }
            return $"http://openweathermap.org/img/wn/{value}@2x.png";
        }
        private string SerializePredictionRequestForPandasDataFrame(PredictionRequest pr)
        {
            string[] columns = { "OriginAirportCode", "Month", "DayofMonth", "CRSDepHour", "DayOfWeek", "Carrier", "DestAirportCode", "WindSpeed", "SeaLevelPressure", "HourlyPrecip" };
            object[] dataRow = { pr.OriginAirportCode, pr.Month, pr.DayofMonth, pr.CRSDepHour, pr.DayOfWeek, pr.Carrier, pr.DestAirportCode, pr.WindSpeed, pr.SeaLevelPressure, pr.HourlyPrecip };
            object[][] data = new object[][] { dataRow };

            var predictionRequestDF = new PredictionRequestDataFrame();
            predictionRequestDF.columns = columns;
            predictionRequestDF.data = data;
            string json = JsonConvert.SerializeObject(predictionRequestDF);
            return json;
        }

        private async Task PredictDelays(DepartureQuery query, ForecastResult forecast)
        {
            if (string.IsNullOrWhiteSpace(mlUrl) || string.IsNullOrWhiteSpace(pat))
            {
                return;
            }

            var departureDate = DateTime.Parse(txtDepartureDate.Text);

            prediction = new DelayPrediction();

            try
            {
                using (var client = new HttpClient())
                {
                    var predictionRequest = new PredictionRequest
                    {
                        OriginAirportCode = query.OriginAirportCode,
                        Month = query.DepartureDate.Month,
                        DayofMonth = query.DepartureDate.Day,
                        CRSDepHour = query.DepartureDate.Hour,
                        DayOfWeek = query.DepartureDayOfWeek,
                        Carrier = query.Carrier,
                        DestAirportCode = query.DestAirportCode,
                        WindSpeed = forecast.WindSpeed,
                        SeaLevelPressure = forecast.Pressure,
                        HourlyPrecip = forecast.Precipitation
                    };

                    client.BaseAddress = new Uri(mlUrl);
                    client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", pat);
                    string json = SerializePredictionRequestForPandasDataFrame(predictionRequest);
                    var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");
                    var response = await client.PostAsync("", content);

                    if (response.IsSuccessStatusCode)
                    {
                        var responseResult = await response.Content.ReadAsStringAsync();
                        var token = JToken.Parse(responseResult);
                        var parsedResult = JsonConvert.DeserializeObject<List<double>>(token.ToString());
                        var result = parsedResult[0];
                        double confidence = 0;
                        if (result == 1)
                        {
                            this.prediction.ExpectDelays = true;
                            this.prediction.Confidence = confidence;
                        }
                        else if (result == 0)
                        {
                            this.prediction.ExpectDelays = false;
                            this.prediction.Confidence = confidence;
                        }
                        else
                        {
                            this.prediction = null;
                        }

                    }
                    else
                    {
                        prediction = null;

                        Trace.Write($"The request failed with status code: {response.StatusCode}");

                        // Print the headers - they include the request ID and the timestamp, which are useful for debugging the failure
                        Trace.Write(response.Headers.ToString());

                        var responseContent = await response.Content.ReadAsStringAsync();
                        Trace.Write(responseContent);
                    }
                }
            }
            catch (Exception ex)
            {
                prediction = null;
                System.Diagnostics.Trace.TraceError("Failed retrieving delay prediction: " + ex.ToString());
                throw;
            }
        }
    }

    #region Data Structures

    public class PredictionRequest
    {
        public string OriginAirportCode { get; set; }
        public int Month { get; set; }
        public int DayofMonth { get; set; }
        public int CRSDepHour { get; set; }
        public int DayOfWeek { get; set; }
        public string Carrier { get; set; }
        public string DestAirportCode { get; set; }
        public double WindSpeed { get; set; }
        public double SeaLevelPressure { get; set; }
        public double HourlyPrecip { get; set; }
    }

    // Our model requires JSON-serialized data in a particular Pandas DataFrame format, with a structure like:
    // '{"columns":["OriginAirportCode", "Month", ...],"data":[["ATL", 09, ...]]}'
    public class PredictionRequestDataFrame
    {
        public string[] columns { get; set; }
        public object[][] data { get; set; }
    }

    public class ForecastResult
    {
        public double WindSpeed;
        public double Precipitation;
        public double Pressure;
        public string ForecastIconUrl;
        public string Condition;
    }

    public class DelayPrediction
    {
        public bool ExpectDelays;
        public double Confidence;
    }

    public class DepartureQuery
    {
        public string OriginAirportCode;
        public double OriginAirportLat;
        public double OriginAirportLong;
        public string DestAirportCode;
        public DateTime DepartureDate;
        public int DepartureDayOfWeek;
        public string Carrier;
    }

    public class Airport
    {
        public string AirportCode { get; set; }
        public double Latitude { get; set; }
        public double Longitude { get; set; }
    }

    #endregion
}
