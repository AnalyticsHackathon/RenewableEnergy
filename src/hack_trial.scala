/**
 * Created by janjayar on 17/02/17.
 */
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.collection.immutable.Range



object hack_trial extends Serializable{

  def main(args: Array[String]): Unit = {
    //Import Data
    // TODO: change hardcoded path
    val path_prefix = "C:\\Users\\porichar\\Work\\hack8\\problem\\"
    val country_ec_co2_path = path_prefix.concat("lcoe-co2.json")
    val wind_energy_path = path_prefix.concat("WindEnergy.csv")
    val geothermal_path = path_prefix.concat("Geothermal.csv")
    val hydropower_path = path_prefix.concat("Hydropower.csv")
    val waste2energy_path = path_prefix.concat("WasteToEnergy.csv")
    val nuclear_path =  path_prefix.concat("Uranium_Nuclear.csv")
    val naturalgas_path = path_prefix.concat("NaturalGas.csv")
    val solarenergy_path = path_prefix.concat("SolarPower.csv")
    val supplier_path = path_prefix.concat("SupplierInformation_revisited.csv")

    //create sparkContext and SqlContext
    val conf = new SparkConf().setAppName("Spark Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new org.apache.spark.sql.SQLContext(sc)
    import sQLContext.implicits._
    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("spark session example")
      .getOrCreate()

    //Read all data into Spark
    val windenergy_df = sparkSession.read
        .format("com.databricks.spark.csv")
        .option("header", "true") //reading the headers
        .option("inferSchema", true)
      .load(wind_energy_path);

    val geothermalenergy_df = sparkSession.read
        .format("com.databricks.spark.csv")
        .option("header", "true") //reading the headers
        .option("inferSchema", true)
      .load(geothermal_path);

    val hydropower_df = sparkSession.read
        .format("com.databricks.spark.csv")
        .option("header", "true") //reading the headers
        .option("inferSchema", true)
      .load(hydropower_path);

    val waste2energy_df = sparkSession.read
        .format("com.databricks.spark.csv")
        .option("header", "true") //reading the headers
        .option("inferSchema", true)
      .load(waste2energy_path);

    val nuclear_df = sparkSession.read
        .format("com.databricks.spark.csv")
        .option("header", "true") //reading the headers
        .option("inferSchema", true)
      .load(nuclear_path);

    val naturalgas_df = sparkSession.read
        .format("com.databricks.spark.csv")
        .option("header", "true") //reading the headers
        .option("inferSchema", true)
      .load(naturalgas_path);

   val solarenergy_df =  sparkSession.read
        .format("com.databricks.spark.csv")
        .option("header", "true") //reading the headers
        .option("inferSchema", true)
     .load(solarenergy_path);

    val supplier_df =  sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "true") //reading the headers
      .option("inferSchema", true)
      .load(supplier_path);


    //load Json file for country ec and co2 function
    val country_emission_cost_DF = sQLContext.read.json(country_ec_co2_path)
    //country_emission_cost_DF.printSchema()
    //country_emission_cost_DF.show()
    val join_supp_final_DF = country_emission_cost_DF.join(supplier_df, Seq("Country"), "inner")
    val join_distinct_country_DF = join_supp_final_DF.select(join_supp_final_DF("Country")).distinct()
    //supplier_df.printSchema()
    //supplier_df.show()

    //Solar Energy - For indicator, we calculated the difference between 2015's solar consumption and 2014's solar generation.
    //There is a lot of data loss between consumption and generation.
    //This will be more meaningful only when we have enough data points for energy generation in 2015 and the loss of energy between consumption
    // and generation for both the years.
    val solar_udf = udf((x: String, y: String) => {
      if (y == "-" || x == "-") -1
      else if ((x.toDouble - y.toDouble) < -100) -1
      else if ((x.toDouble - y.toDouble) < 100) 0
      else 1
    })
    val join_solar_final_DF = join_distinct_country_DF.join(solarenergy_df, Seq("Country"), "left")
    val solarenergyintemediate_compare = join_solar_final_DF.withColumn("solarenergy_indicator",(solar_udf(join_solar_final_DF("Total Solar Electricity Consumption(GWh) in 2015 BP (2016)"), join_solar_final_DF("Total Solar Electricity Generation in 2014 (GWh) IRENA (2016)"))))
    val solarenergyfinal = solarenergyintemediate_compare.select("Country", "solarenergy_indicator", "Share of Solar PV in Total Installed Power Generating Capacity in 2015 (%)BP (2016)")
    //solarenergyfinal.show()
    //solarenergyfinal.printSchema()


    //WIND ENERGY  - similar to solar.
    val percent_udf = udf((x: String, y: String) => {
      if (y == "-" || x == "-") -1
      else if ((x.toDouble - y.toDouble) < -100) -1
      else if ((x.toDouble - y.toDouble) < 100) 0
      else 1
    })
    val join_wind_final_DF = join_distinct_country_DF.join(windenergy_df, Seq("Country"), "left")
    val percent_energy = join_wind_final_DF
      .withColumn("total_energy_2015 in MW", ((join_wind_final_DF("Total Wind Installed Capacity (MW) in 2015 IRENA (2016)") *100)/join_wind_final_DF("Share of Wind in Total Installed Capacity in 2015 ()79 BP (2016)")))
    val windenergyintemediate_compare = percent_energy.withColumn("windenergy_indicator",(percent_udf(percent_energy("Wind Energy Consumption In 2015 Gigawatt-hours (GWh)7778 BP (2016)"), percent_energy("Total Wind Energy Generation in 2014 (GWh) IRENA (2016)"))))
    val windenergyfinal = windenergyintemediate_compare.select("Country", "windenergy_indicator", "Share of Wind in Total Installed Capacity in 2015 ()79 BP (2016)", "total_energy_2015 in MW")
    //windenergyfinal.show()


    //HYDRO ENERGY - The factor is calculated between energy generated and consumed between the same year.
    //This can give you the indication of how much more hydropower can be consumed.
    val hydro_udf = udf((x: String, y: String) => {
      if(x.isEmpty() || x == "-" || x.toDouble < 100) -1
      else if (y.isEmpty() || y == "-") 1
      else if (x.toDouble - y.toDouble < 100)0
      else 1
    })
    val join_hydro_final_DF = join_distinct_country_DF.join(hydropower_df, Seq("Country"), "left")
    val hydropowedfintremediate = join_hydro_final_DF.join(percent_energy,usingColumn = "Country")
    val hydint = hydropowedfintremediate
      .withColumn("Share of hydro in Total Installed Capacity in 2015 ()79 BP (2016)", (hydropowedfintremediate("Total Hydropower Capacity (MW) in 2015")*100)/hydropowedfintremediate("total_energy_2015 in MW"))
      .withColumn("hydro_indicator",(hydro_udf(hydropowedfintremediate("Estimated Net Hydropower Hydroelectricity Generation (GWh) in 2015   BP (2016)"), hydropowedfintremediate("Consumption of (GWh) in 2015"))) )
    val hydropowerfinal = hydint.select("Country", "hydro_indicator", "Share of hydro in Total Installed Capacity in 2015 ()79 BP (2016)")
    //hydropowerfinal.printSchema()
    //hydropowerfinal.show()


    //GEOTHERMAL ENERGY - Geothermal energy being nascent, the indicator says if it can be used at all for electricity
    val geo_udf = udf((x: String) => {
      val y = x
      if (y == "-") -1
      else if (y.toDouble < 10) 0
      else 1
    })
    val join_geo_final_DF = join_distinct_country_DF.join(geothermalenergy_df, Seq("Country"), "left")
    val geothermalenergy_compare = join_geo_final_DF.withColumn("geo_indicator",(geo_udf(join_geo_final_DF("Total Electricity Generating Capacity (MW) in 2015"))))
    val geothermalenergy_final = geothermalenergy_compare.select("Country", "geo_indicator")
    //geothermalenergy_final.show()


    //Uranium nuclear energy - Nuclear energy indicator is based on how many new reactors are coming up.
    // This shows indication of how much nuclear enrgy will be available in future for consumption.
    val nuclear_udf = udf((x: Int) => {
      val y = x
      if (y == 0) -1
      else if (y < 3) 0
      else 1
    })
    //nuclear_df.printSchema()
    val join_nuclear_final_DF = join_distinct_country_DF.join(nuclear_df, Seq("Country"), "left")
    val nuclearint = join_nuclear_final_DF
      .withColumn("Share of uranium nuclear in Total Installed Capacity in 2015 ()79 BP (2016)", join_nuclear_final_DF(" of Total"))
      .withColumn("nuclear_indicator", nuclear_udf(join_nuclear_final_DF("Reactors Under Construction - No Of Reactors3")))
    val nuclearfinal =  nuclearint.select("Country", "nuclear_indicator", "Share of uranium nuclear in Total Installed Capacity in 2015 ()79 BP (2016)")
    //nuclearfinal.show()


    //Waste2Energy Indicator - Similar to geothermal energy, being a nascent field.
    val waste2energy_udf = udf((x: String) => {
      val y = x
      if (y == null || y.isEmpty() || y == "" || y == "-") -1
      else if (y.toDouble < 100) 0
      else 1
    })
    val join_waste_final_DF = join_distinct_country_DF.join(waste2energy_df, Seq("Country"), "left")
    val waste2energy_compare = join_waste_final_DF.withColumn("waste2energy_indicator",(waste2energy_udf(join_waste_final_DF("Electrical Generating Capacity (MW) in 2015  (GWh) from RMW in IRENA (2016)"))))
    val waste2energy_final = waste2energy_compare.select("Country", "waste2energy_indicator")
    //waste2energy_final.show()


    // Natural Gas Indicator
    // This is non-renewable and draining fast.
    // Indicator of how long this can act as a sustainable source of energy.
    val naturalgas_udf = udf((x: Double) => {
      val y = x
      if (y < 20) -1
      else if (y < 50) 0
      else 1
    })
    val join_ngas_final_DF = join_distinct_country_DF.join(naturalgas_df, Seq("Country"), "left")
    val naturalgas_compare = join_ngas_final_DF.withColumn("naturalgasenergy_indicator",(naturalgas_udf(join_ngas_final_DF("R/P0Ratio0Years"))))
    val naturalgas_final = naturalgas_compare.select("Country", "naturalgasenergy_indicator")
    //naturalgas_final.show()

//    val country_emission_cost_DF_as = country_emission_cost_DF.as("country_emission_cost")
//    val naturalgas_final_as = naturalgas_final.as("naturalgas_final")
//    //val joined_DF = country_emission_cost_DF_as.join(naturalgas_final_as, col("country_emission_cost.Country") === col("naturalgas_final.Country"), "inner")
    //val joined_DF = country_emission_cost_DF.join(naturalgas_final, (country_emission_cost_DF.col("Country") === naturalgas_final.col("Country")), "inner")
    //val join_count_ngas_DF = country_emission_cost_DF.join(naturalgas_final, Seq("Country"))
    //join_count_ngas_DF.printSchema()
    //join_count_ngas_DF.show()


    //join_supp_final_DF.printSchema()
    //join_supp_final_DF.show()

    val join_final_DF = join_supp_final_DF.join(naturalgas_final, Seq("Country"), "left")
      .join(waste2energy_final, Seq("Country"), "left")
      .join(nuclearfinal, Seq("Country"), "left")
      .join(geothermalenergy_final, Seq("Country"), "left")
      .join(hydropowerfinal, Seq("Country"), "left")
      .join(windenergyfinal, Seq("Country"), "left")
      .join(solarenergyfinal, Seq("Country"), "left")

    //join_final_DF.show()
    //join_final_DF.printSchema()

    val grid_co2e_udf = udf((shr_nuc: Double, shr_hyd: Double, shr_wnd: String, shr_slr: String, coal: Long, nuc: Long, hyd: Double, wnd: Long, slr: Long, supp_ene: Int) => {
      var shr_wnd_int: Double = 0
      if (shr_wnd == "-") shr_wnd_int = 0
      else shr_wnd_int = shr_wnd.toDouble

      var shr_slr_int: Double = 0
      if (shr_slr == "-") shr_slr_int = 0
      else shr_slr_int = shr_wnd.replace("%","").toDouble

      var fossil_per: Double = 100 - (shr_nuc + shr_hyd + shr_wnd_int + shr_slr_int)
      if (fossil_per < 0) fossil_per = 0

      //Return the total carbon foot print in tons of CO2e / year
      ((shr_nuc * nuc * supp_ene) + (shr_hyd * hyd * supp_ene) + (shr_wnd_int * wnd * supp_ene) + (shr_slr_int * slr * supp_ene) + (fossil_per * coal * supp_ene)) / (1000 * 1000 * 1000)
    })

    val grid_lcoe_udf = udf((shr_nuc: Double, shr_hyd: Double, shr_wnd: String, shr_slr: String, coal: Long, nuc: Long, hyd: Double, wnd: Long, slr: Long, supp_ene: Int) => {
      var shr_wnd_int: Double = 0
      if (shr_wnd == "-") shr_wnd_int = 0
      else shr_wnd_int = shr_wnd.toDouble

      var shr_slr_int: Double = 0
      if (shr_slr == "-") shr_slr_int = 0
      else shr_slr_int = shr_wnd.replace("%","").toDouble

      var fossil_per: Double = 100 - (shr_nuc + shr_hyd + shr_wnd_int + shr_slr_int)
      if (fossil_per < 0) fossil_per = 0

      ((shr_nuc * nuc * supp_ene) + (shr_hyd * hyd * supp_ene) + (shr_wnd_int * wnd * supp_ene) + (shr_slr_int * slr * supp_ene) + (fossil_per * coal * supp_ene)) / 10
    })

    val combine_DF = join_final_DF.withColumn("Grid.tCO2e/yr", grid_co2e_udf(join_final_DF("Share of uranium nuclear in Total Installed Capacity in 2015 ()79 BP (2016)"),
      join_final_DF("Share of hydro in Total Installed Capacity in 2015 ()79 BP (2016)"),
      join_final_DF("Share of Wind in Total Installed Capacity in 2015 ()79 BP (2016)"),
      join_final_DF("Share of Solar PV in Total Installed Power Generating Capacity in 2015 (%)BP (2016)"),
      join_final_DF("`Coal.CO2e`"),
      join_final_DF("`Nuclear.CO2e`"),
      join_final_DF("`Hydro.CO2e`"),
      join_final_DF("`Wind.CO2e`"),
      join_final_DF("`Solar.CO2e`"),
      join_final_DF("Total electricity consumed in 2015 (Kwh/yr)")))
      .withColumn("Grid.LCOe", grid_lcoe_udf(join_final_DF("Share of uranium nuclear in Total Installed Capacity in 2015 ()79 BP (2016)"),
        join_final_DF("Share of hydro in Total Installed Capacity in 2015 ()79 BP (2016)"),
        join_final_DF("Share of Wind in Total Installed Capacity in 2015 ()79 BP (2016)"),
        join_final_DF("Share of Solar PV in Total Installed Power Generating Capacity in 2015 (%)BP (2016)"),
        join_final_DF("`Coal.LCoE`"),
        join_final_DF("`Nuclear.LCoE`"),
        join_final_DF("`Hydro.LCoE`"),
        join_final_DF("`Wind.LCoE`"),
        join_final_DF("`Solar.LCoE`"),
        join_final_DF("Total electricity consumed in 2015 (Kwh/yr)")))
    val combine_tot_DF = combine_DF.withColumn("Total.tCO2e/yr", combine_DF("`Grid.tCO2e/yr`") + combine_DF("Total carbon emissions from external sources in 2015 (tons/yr)"))
      .withColumn("Total.LCOE", combine_DF("`Grid.LCOe`") + combine_DF("Total LCOe from external sources"))
    //combine_tot_DF.show()
    //combine_tot_DF.printSchema()
    //val selectedColumnName = combine_tot_DF.columns(47) //pull the qth column from the columns array
    //println(selectedColumnName)

    val m_lcoe_agg = combine_tot_DF.agg(min("`Total.LCOE`").alias("min_total_lcoe"), max("`Total.LCOE`").alias("max_total_lcoe"))
    val m_co2e_agg = combine_tot_DF.agg(min("`Total.tCO2e/yr`").alias("min_total_coe2"), max("`Total.tCO2e/yr`").alias("max_total_coe2"))
    //m_lcoe_agg.printSchema()
    //m_co2e_agg.printSchema()
    val min_total_lcoe = m_lcoe_agg.select("min_total_lcoe").first().toString().replace("[","").replace("]","").toDouble
    val max_total_lcoe = m_lcoe_agg.select("max_total_lcoe").first().toString().replace("[","").replace("]","").toDouble

    val gpa_lcoe_udf = udf ((value: Double) => {
      val new_min: Double = 1
      val new_max: Double = 10
      (((value - min_total_lcoe) * (new_max - new_min)) / (max_total_lcoe - min_total_lcoe)) + new_min
    })

    val min_total_coe2 = m_co2e_agg.select("min_total_coe2").first().toString().replace("[","").replace("]","").toDouble
    val max_total_coe2 = m_co2e_agg.select("max_total_coe2").first().toString().replace("[","").replace("]","").toDouble

    val gpa_coe2_udf = udf ((value: Double) => {
      val new_min: Double = 1
      val new_max: Double = 10
      (((value - min_total_coe2) * (new_max - new_min)) / (max_total_coe2 - min_total_coe2)) + new_min
    })

    val gpa_final_DF = combine_tot_DF.withColumn("GPA.LCOE", gpa_lcoe_udf(combine_tot_DF("`Total.LCOE`")))
      .withColumn("GPA.tCO2e/yr", gpa_coe2_udf(combine_tot_DF("`Total.tCO2e/yr`")))

    //gpa_final_DF.show()
    //gpa_final_DF.printSchema()
    val pred_10e_in_udf = udf ((wast: Double, geo: Double, hydro: Double, wind: Double, slr: Double, fossil: Double, tot_e: Double) => {
      var max = wast
      if (max < geo) max = geo
      if (max < hydro) max = hydro
      if (max < wind) max = wind
      if (max < slr) max = slr
      val per10 = tot_e / 10
      ((per10 / fossil) - (per10 / max)) / (1000 * 1000)
    })
    val pred_10e_inc_DF = gpa_final_DF.withColumn("Amount of % tCO2e/yr can be saved for 10% inc in cost",
      pred_10e_in_udf((gpa_final_DF("waste2energy_indicator") * gpa_final_DF("`WasteToE.LCoE`")) / gpa_final_DF("`WasteToE.CO2e`"),
        (gpa_final_DF("geo_indicator") * gpa_final_DF("`Geo.LCoE`")) / gpa_final_DF("`Geo.CO2e`"),
        (gpa_final_DF("hydro_indicator") * gpa_final_DF("`Hydro.LCoE`")) / gpa_final_DF("`Hydro.CO2e`"),
        (gpa_final_DF("windenergy_indicator") * gpa_final_DF("`Wind.LCoE`")) / gpa_final_DF("`Wind.CO2e`"),
        (gpa_final_DF("solarenergy_indicator") * gpa_final_DF("`Solar.LCoE`")) / gpa_final_DF("`Solar.CO2e`"),
        gpa_final_DF("`Coal.LCoE`") / gpa_final_DF("`Coal.CO2e`"), gpa_final_DF("`Total.LCOE`")
      ))

    pred_10e_inc_DF.show()
    pred_10e_inc_DF.printSchema()
  }
}