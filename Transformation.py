import pandas as pd

def run_transformation():
    data = pd.read_csv('zipco_transaction.csv')
    # Data Cleaning and transformation
    # Remove duplicates
    data.drop_duplicates(inplace=True)

    # Handling missing values (filling missing numeric values with the mean or median)
    numeric_columns = data.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_columns:#
        data.fillna({col:data[col].mean()}, inplace=True)

    # Handle missing values(fill missing string or object values with 'Unknown')
    string_columns = data.select_dtypes(include=['object']).columns
    for col in string_columns:
        data.fillna({col:'Unknown'}, inplace=True) 

    #Convert 'Date' type to datetime
    data['Date'] = pd.to_datetime(data['Date'])

    # Data Transformation
    # Create the product table
    product = data[['ProductName']].drop_duplicates().reset_index(drop=True)
    product.index.name = 'ProductID'
    product = product.reset_index()

    # Create Customers Table
    customer = data[['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber','CustomerEmail', 'CustomerFeedback']].drop_duplicates().reset_index(drop=True)
    customer.index.name = 'CustomerID'
    customer = customer.reset_index()

    # Create Staff table
    staff = data[['Staff_Name', 'Staff_Email', 'StaffPerformanceRating']].drop_duplicates().reset_index(drop=True)
    staff.index.name = 'StaffID'
    staff = staff.reset_index()

    # Create transaction table
    transaction = data.merge(product, on=['ProductName'], how='left') \
                    .merge(customer, on=['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber','CustomerEmail', \
                        'CustomerFeedback'], how='left') \
                    .merge(staff, on=['Staff_Name', 'Staff_Email', 'StaffPerformanceRating'], how='left')

    transaction.index.name = 'TransactionID'
    transaction = transaction.reset_index() \
                            [['Date', 'TransactionID', 'ProductID', 'Quantity', 'UnitPrice', 'StoreLocation','PaymentType', \
                                'PromotionApplied', 'Weather', 'Temperature', 'DeliveryTime_min','OrderType', 'CustomerID', \
                                    'StaffID', 'DayOfWeek','TotalSales']]

    # Save date as csv file
    data.to_csv('clean_data.csv', index=False)
    product.to_csv('product.csv', index=False)
    customer.to_csv('customer.csv', index=False)
    staff.to_csv('staff.csv', index=False)
    transaction.to_csv('transaction.csv', index=False)

    print('Data Cleaning and Transformation completed Successful')