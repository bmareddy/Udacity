#
#TODO: Make this a json file
#

aqi_parameter_breakpoints = {
    44201: {
        "name": "Ozone",
        "abbreviation": "O3",
        "units": "ppm",
        "breakpoints": {
            "8-hour": {
                "0": (0.000, 0.055),
                "1": (0.055, 0.071),
                "2": (0.071, 0.086),
                "3": (0.086, 0.106),
                "4": (0.106, 0.200)
            },
            "1-hour": {
                "2": (0.125, 0.165),
                "3": (0.165, 0.205),
                "4": (0.205, 0.405),
                "5": (0.405, 0.505),
                "6": (0.505, 0.604)
            }
        }
    },
    42401: {
        "name": "Sulphur Dioxide",
        "abbreviation": "SO2",
        "units": "ppb",
        "breakpoints": {
            "1-hour": {
                "0": (0, 36),
                "1": (36, 76),
                "2": (76, 186),
                "3": (186, 305),
                "4": (305, 605),
                "5": (605, 805),
                "6": (805, 1004)                
            }
        }
    },
    42101: {
        "name": "Cabon Monoxide",
        "abbreviation": "CO",
        "units": "ppm",
        "breakpoints": {
            "8-hour": {
                "0": (0.0, 4.5),
                "1": (4.5, 9.5),
                "2": (9.5, 12.5),
                "3": (12.5, 15.5),
                "4": (15.5, 30.5),
                "5": (30.5, 40.5),
                "6": (40.5, 50.4)
            }
        }
    },
    42602: {
        "name": "Nitrogen Dioxide",
        "abbreviation": "NO2",
        "units": "ppb",
        "breakpoints": {
            "1-hour": {
                "0": (0, 55),
                "1": (54, 101),
                "2": (101, 361),
                "3": (361, 650),
                "4": (650, 1250),
                "5": (1250, 1650),
                "6": (1650, 2049)                
            }
        }
    },
    88101: {
        "name": "PM2.5",
        "abbreviation": "PM2.5",
        "units": "Micrograms / cubic meter",
        "breakpoints": {
            "24-hour": {
                "0": (0, 12.1),
                "1": (12.1, 35.5),
                "2": (35.5, 55.5),
                "3": (55.5, 150.5),
                "4": (150.5, 250.5),
                "5": (250.5, 350.5),
                "6": (350.5, 500.4)                
            }
        }
    },
    81102: {
        "name": "PM10",
        "abbreviation": "PM10",
        "units": "Micrograms / cubic meter",
        "breakpoints": {
            "24-hour": {
                "0": (0, 55),
                "1": (55, 155),
                "2": (155, 255),
                "3": (255, 355),
                "4": (355, 425),
                "5": (425, 505),
                "6": (505, 504)                 
            }
        }
    }
}

aqi_category_lookup = {
    "0": "Good",
    "1": "Moderate",
    "2": "Unhealthy For Sensitive Groups",
    "3": "Unhealthy",
    "4": "Very Unhealthy",
    "5": "Hazardous",
    "6": "Hazardous"
}