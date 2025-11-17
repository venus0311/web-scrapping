all_job_levels = {
            'Lower Managment (Employee)': ['Intern', 'Analyst', 'Specialist', 'Consultant', 'Planner', 'Agent', 'Associate', 'Auditor', 'Defender', 'Buyer', 'Scientist', 'Operator', 'Coordinator', 'Accountant', 'Representative', 'Recruiter', 'Designer', 'Inspector', 'MD (Medical Doctor)', 'Officer', 'Secretary', 'Developer', 'Strategist', 'Expert', 'Technician', 'Writer', 'Professor', 'Programmer', 'Operator', 'Producer', 'Administrator', 'Admin', 'Engineer'],
            'Senior Lower Managment (Sr Employee)': ['Supervisor', 'Chief', 'Senior', 'Lead', 'Principal', 'Sr'],
            'Middle Managment (Manager)': ['Lead', 'Leader', 'Team Lead','Manager', 'Chief', "Mgr", "Mngr",'Architect','Product Owner', 'Applications Owner', 'Business Owner'],
            'Senior Middle Management (Sr Manager)': ['Senior Manager', 'Senior Team Leader', 'Senior Team Lead', 'Sr mngr', 'Sr manager', 'Srmngr', 'Srmanager', 'Assistant Director','Associate Director', 'Executive', 'Senior Executive'],
            'Senior Management (Director)': ['Director', 'Head', 'Principal', 'General manager', 'Executive Director', 'Managing Director', 'Executive', 'Controller', 'Comptroller', 'Treasurer', 'Counsel', 'Deputy Director', 'Senior Director', 'Sr dir', 'Srdir', 'Srdirector', 'Sr director', 'Associate Vice President', 'Assistant Vice President', 'AVP', 'Vice President', 'VP', 'Senior Vice President', 'SVP', 'Executive Vice President', 'EVP', 'President', 'Superintendent'],
            'C Level': ['Chief Information Officer', 'CIO', 'Chief Information Security Officer', 'CISO', 'Chief Security Officer', 'CSO', 'Chief Technology Officer', 'CTO', 'Chief Operations Officer', 'COO', 'Chief Executive Officer', 'CEO', 'Chief Administrative Officer', 'CAO', 'Chief Human Resources Officer', 'CHRO', 'Chief Finance Officer', 'Chief Financial Officer', 'CFO', 'Chief Marketing Officer', 'CMO', 'Chief Product Officer', 'CPO', 'Chief Demand Officer', 'CDO', 'Chief Development Officer', 'Chief Digital Officer', 'Chief Data Officer', 'Chief Brand Officer', 'CBO', 'Chief Accounting Officer', 'CAO', 'Chief Revenue Officer', 'CRO', 'Chief Investment Officer', 'Chief Financial Planning Officer', 'CFPO', 'Chief Risk Officer', 'CRO', 'Chief Audit Executive', 'CAE', 'Chief Treasury Officer', 'CTO', 'Chief Compliance Officer', 'CCO', 'Chief Tax Officer', 'Chief Customer Officer', 'CCO', 'Chief Client Officer', 'CLO', 'Chief Customer Support Officer', 'CCSO', 'Chief Service Officer', 'CSO', 'Chief Talent Officer', 'Chief People Officer', 'CPO', 'Chief Human Resource Officer', 'CHRO', 'Chief Talent Officer', 'Chief Commercial Officer', 'CCO', 'Chief Sales Officer', 'Chief Operating Officer', 'Chief Operation Officer', 'Chief Ops Officer', 'Chief Logistics Officer', 'CLO', 'Chief Supply Chain Officer', 'CSCO', 'Chief Transportation Officer', 'Chief Insurance Officer', 'Chief Risk Officer', 'CRO', 'Chief Underwriting Officer', 'CUO', 'Chief Claims Officer', 'CCO', 'Chief Actuary Officer', 'Chief Medical Officer', 'CMO', 'Chief Health Officer', 'CHO', 'Chief Clinical Officer', 'CCO', 'Chief Nursing Officer', 'CNO', 'Chief Patient Experience Officer', 'CPEO', 'Chief Health Information Officer', 'CHIO', 'Chief Wellness Officer', 'CWO', 'Chief Pharmacy Officer', 'CPO', 'Chief Epidemiology Officer', 'Chief Academic Officer', 'Chief Education Officer', 'Chief Procurement Officer', 'Chief Sourcing Officer', 'Chief Purchasing Officer', 'Chief Legal Officer', 'CLO', 'Chief Real Estate Officer', 'CREO', 'Chief Manufacturing Officer', 'Chief Production Officer', 'Chief Quality Officer', 'CQO', 'Chief Governance Officer', 'CGO', 'Chief Hospitality Officer', 'CHO'],
            'Ownership': ['Owner', 'Founder', 'Shareholder', 'Co-founder', 'co-owner', 'Partner', 'Board Member', 'Chair', 'Advisor', 'Counsel', 'Chairman', 'Chairwoman']
        }


job_level_seniority = ["Lower Managment (Employee)", "Senior Lower Managment (Sr Employee)", "Middle Managment (Manager)", "Senior Middle Management (Sr Manager)", "Senior Management (Director)", "C Level", "Ownership"]


equal_levels_map  = {
    'Lead': ['Leader', 'Team Lead', 'Manager', 'Chief'],
    'Senior Manager': ['Senior Team Leader', 'Senior Team Lead', 'Sr Mgr', 'SrMngr', 'Sr.Mngr', 'Sr.'],
    'Assistant Director': ['Associate Director'],
    'Product Owner': ['Applicatons Owner', 'Business Owner'],
    'Analyst': ['Specialist', 'Consultant', 'Planner', 'Lawyer', 'Attorney', 'Auditor', 'Defender', 'Buyer', 'Scientist', 'Operator', 'Instructor', 'Expeditor', 'Coordinator', 'Accountant', 'Representative', 'Recruiter', 'Designer', 'Inspector', 'MD (Medical Doctor)', 'Officer', 'Secretary', 'Merchandiser', 'Trainer', 'Agent', 'Developer', 'Strategist', 'Expert', 'Technician', 'Writer', 'Professor', 'Programmer', 'Producer', 'Master', 'Associate'],
    'Director': ['Head', 'Principal', 'General manager', 'Executive Director', 'Managing Director', 'MD', 'GM', 'Controller', 'Comptroller', 'Treasurer', 'Counsel', 'Deputy director'],
    'AVP': ['Associate Vice President', 'Assistant Vice President'],
    'VP': ['Vice President'],
    'SVP': ['Senior Vice President'],
    'EVP': ['Executive Vice President'],
    'CIO': ['Chief Information Officer'],
    'CISO': ['Chief Information Security Officer'],
    'CSO': ['Chief Security Officer'],
    'CTO': ['Chief Technology Officer'],
    'COO': ['Chief Operations Officer'],
    'CEO': ['Chief Executive Officer'],
    'CHRO': ['Chief Human Resources Officer'],
    'CFO': ['Chief Finance Officer'],
    'CMO': ['Chief Marketing Officer'],
    'CPO': ['Chief Product Officer'],
    'CDO': ['Chief Demand Officer', 'Chief Development Officer', 'Chief Digital Officer', 'Chief Data Officer'],
    'Owner': ['Founder', 'Shareholder', 'Co-founder', 'Partner', 'Board Member', 'Chair', 'Advisor', 'Counsel']
}