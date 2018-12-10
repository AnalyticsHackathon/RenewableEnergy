# RenewableEnergy
Data analysis and prediction of which is the most carbon efficient country in the future

# Why & What?
Develop a Score-Card modeling tool that takes industry data – analyzes and scores Cisco’s suppliers.
* Through accredited data sources on renewables and the availability of these renewable energy sources by country, a complete assessment can be completed of Cisco’s Supply Chain and a scorecard generated to help drive the future of moving towards a Sustainable Supply Chain while helping to reduce CO2 emissions.

# Objectives
* Rank Suppliers against each other
* Provide Tools to nudge a Supplier 
  * To utilize the Green Potential available in his region
  * To open up in areas with higher Green Potential
* Mechanism to improve a suppliers CO2e against the investments he is willing to make – Investment-Benefit Analysis
* Arm Tools to negotiate with local bodies where our Suppliers are willing to open up new plants

# Public Domain – Industry Data
* IRENA (resourceirena.irena.org)
  * Global Data on all renewable energy + data models for RE investment
* NREL (www.nrel.gov)
  * US Specific granular info on energy production/consumption
* CEA(www.cea.nic.in) 
  * India specific energy information
* Fraunhofer ISE (www.ise. fraunhofer.de)
  * German specific Solar data
* Worldbank (data.worldbank.org)
  * Country-wise CO2e, RE use
* Technology-wise RE potential – multiple sources NREL, CEA, Worldbank, IRENA 

# Data points Considered
* Electricity, Own Solar panel, Diesel, Gas
* LCoE for each source
* CO2-Equivalent emision for each source
* RE composition of Electricity from Grid
* Energy consumption percentage
* Climate Control, Vehicle, Lighting & Appliances

# Data Sources
Region-wise spread of Renewable Energy | Region & Tech-wise cost (USD/kWh)+ CO2 Emission (g/kWh) | Supplier Energy consumption
---------------------------------------|---------------------------------------------------------|----------------------------
USA NW– 11.1% Solar, 17.2% Wind | Solar - Cost: 0.12, CO2e: 54 Wind – Cost: 0.10, CO2e: 12 | Toshiba– Elec: 9MWh/yr Solar: 10MWh/yr Fossil: 14MWh/yr
Germany – 10.4% Wind, 17.2% Solar | Solar - Cost: 0.12, CO2e: 54 Wind – Cost: 0.11, CO2e: 12 | Leoni– Elec: 50MWh/yr Solar: 1MWh/yr Fossil: 2MWh/yr
China – 33.4% Wind, 18.9% Solar | Solar - Cost: 0.10, CO2e: 52 Wind – Cost: 0.10, CO2e: 11 | AVC – Elec: 12MWh/yr Fossil: 0.3MWh/yr 
Spain – 5.3% Wind, 2.4% Solar | Solar - Cost: 0.11, CO2e: 54 Wind – Cost: 0.10, CO2e: 12 | Vector – Elec:10MWh/yr Fossil: 2MWh/yr 
Brazil – 2% Wind, 2.8% Nuclear | Nuclear- Cost: 0.15, CO2e: 12 Wind – Cost: 0.12, CO2e: 13 | Interprint – Elec: 30MWh/yr Solar: 3.5MWh/yr Fossil: 0.5MWh/yr
Japan – 0.7% Wind, 15.4% Solar | Solar - Cost: 0.12, CO2e: 54 Wind – Cost: 0.12, CO2e: 14 | NEC – Elec: 20MWh/yr Solar: 1MWh/yr Biomass – 0.5MWh/yr

# Algorithm
![alt text](https://github.com/AnalyticsHackathon/RenewableEnergy/blob/master/doc/algorithm_EnergyPotential.png)
![alt text](https://github.com/AnalyticsHackathon/RenewableEnergy/blob/master/doc/algorithm_TotalCost.png)
![alt text](https://github.com/AnalyticsHackathon/RenewableEnergy/blob/master/doc/algorithm_TotalEmission.png)
* Minimizing the 2nd function gives us the most cost optimal solution which may not be green
* Minimizing the 3rd function gives us the most green solution

# Results
https://github.com/AnalyticsHackathon/RenewableEnergy/blob/master/doc/RenewableEnergy-SEMI-FINAL.pptx 
