'''
Created on Jul 6, 2015

@author: shaunz

Data can be queried from Quandl
'''

Fred_ticker_list = [
               #CPI US
               ['cpi_us',                   'CPIAUCNS'      ],
               ['cpi_us_food_at_home',      'CUUR0000SAF11' ],
               ['cpi_us_food_away_home',    'CUUR0000SEFV'  ],
               ['cpi_us_rent',              'CUUR0000SEHA'  ],
               ['cpi_us_owners_equivalent', 'CUUR0000SEHC'  ],
               ['cpi_us_household_fuel',    'CUUR0000SAH2'  ],
               ['cpi_us_apparel',           'CPIAPPNS'      ],
               ['cpi_us_motor',             'CUUR0000SETA'  ],
               ['cpi_us_motor_fuel',        'CUUR0000SETB'  ],
               ['cpi_us_public_transport',  'CUUR0000SETG'  ],
               ['cpi_us_medical_commod',    'CUUR0000SAM1'  ],
               ['cpi_us_medical_service',   'CUUR0000SAM2'  ],
               ['cpi_us_education',         'CUUR0000SAE1'  ],
               ['cpi_us_communication',     'CUUR0000SAE2'  ],
               
               #Wages US
               ['wage_us_employee',                 'PAYEMS'        ],    
               ['wage_us_hours',                    'AWHAETP'       ],    
               ['wage_us_earnings',                 'CES0500000003' ],
               ['wage_us_employee_nonsupervisory',  'CES0500000006' ],    
               ['wage_us_hours_nonsupervisory',     'AWHNONAG'      ],    
               ['wage_us_earnings_nonsupervisory',  'AHETPI'        ],
               ['wage_us_weekly',                   'CES0500000011' ],
               ['wage_us_weekly_nonsupervisory',    'CES0500000030' ],
               ]