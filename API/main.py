from fastapi import FastAPI, HTTPException
import pymssql
from fastapi.responses import FileResponse
from pydantic import BaseModel
import json
import os
app = FastAPI()

# Функция для чтения конфигурации из файла
def load_config(file_path):
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config

# Загрузка конфигурации
config = load_config('config.json')

# Применение конфигурации
DATABASE_CONFIG = {
    "server": config["server"],
    "database": config["database"],
    "user": config["user"],
    "password": config["password"]
}

# Функция для выполнения SQL-запросов
def execute_query(query: str, fetch_results: bool = False):
    try:
        with pymssql.connect(**DATABASE_CONFIG) as conn:
            with conn.cursor(as_dict=True) as cursor:
                cursor.execute(query)
                if fetch_results:
                    results = cursor.fetchall()
                    return results
                else:
                    conn.commit()  # Commit changes for modification queries
                    return {"status": "success", "message": "Operation successful!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Объекты Pydantic моделей (если нужны)

# CRUD операции с использованием FastAPI

@app.get("/")
def read_root():
    return {"message": "Welcome to the Airport API"}

@app.get("/employees")
def get_all_employees():
    query = "SELECT * FROM Airport.Employees;"
    return execute_query(query, fetch_results=True)

@app.delete("/employees/{employee_id}")
def delete_employee(employee_id: int):
    query = f"DELETE FROM Airport.Employees WHERE EmployeeID = {employee_id};"
    result = execute_query(query)
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result

@app.get("/departments/managers")
def get_department_managers():
    query = """
    SELECT E.*
    FROM Airport.Employees E
    INNER JOIN Airport.Departments D ON E.EmployeeID = D.ManagerID;
    """
    return execute_query(query)

@app.get("/departments/{department_id}/employees")
def get_employees_by_department(department_id: int):
    query = f"SELECT * FROM Airport.Employees WHERE DepartmentID = {department_id};"
    return execute_query(query)

@app.get("/employees/gender")
def get_employees_by_gender():
    query = "SELECT * FROM Airport.Employees ORDER BY Gender;"
    return execute_query(query)

@app.get("/employees/age")
def get_employees_by_age():
    query = "SELECT *, DATEDIFF(YEAR, BirthDate, GETDATE()) AS Age FROM Airport.Employees ORDER BY Age;"
    return execute_query(query)

@app.get("/employees/children")
def get_employees_by_children():
    query = "SELECT * FROM Airport.Employees WHERE (ChildrenCount > 0) ORDER BY ChildrenCount;"
    return execute_query(query)

@app.get("/employees/salary")
def get_employees_by_salary():
    query = "SELECT * FROM Airport.Employees ORDER BY Salary;"
    return execute_query(query)

@app.get("/employees/experience")
def get_employees_by_experience():
    query = "SELECT * FROM Airport.Employees ORDER BY WorkExperience;"
    return execute_query(query)

@app.get("/brigades/employees")
def get_brigades_employees():
    query = """
    SELECT *, (SELECT COUNT(*) FROM Airport.Employees WHERE BrigadeID = B.BrigadeID) AS TotalEmployees
    FROM Airport.Brigades B;
    """
    return execute_query(query)

@app.get("/departments/employees/total")
def get_total_employees_by_department():
    query = """
    SELECT D.DepartmentName, COUNT(E.EmployeeID) AS TotalEmployees
    FROM Airport.Departments D
    LEFT JOIN Airport.Employees E ON D.DepartmentID = E.DepartmentID
    GROUP BY D.DepartmentName;
    """
    return execute_query(query)

@app.get("/departments/{department_name}/employees/total")
def get_total_employees_in_department(department_name: str):
    query = f"""
    SELECT *, (SELECT COUNT(*) FROM Airport.Employees WHERE DepartmentID = D.DepartmentID) AS TotalEmployees
    FROM Airport.Departments D
    WHERE D.DepartmentName = '{department_name}';
    """
    return execute_query(query)

@app.get("/flights/{flight_id}/employees")
def get_employees_by_flight(flight_id: int):
    query = f"""
    SELECT E.*, COUNT(*) OVER() AS TotalEmployees
    FROM Airport.Employees E
    JOIN Airport.Brigades B ON E.BrigadeID = B.BrigadeID
    JOIN Airport.Aircrafts A ON B.BrigadeID IN (A.PilotBrigadeID, A.TechnicianBrigadeID, A.ServiceBrigadeID)
    JOIN Airport.Flights F ON F.AircraftID = A.AircraftID
    WHERE F.FlightID = {flight_id};
    """
    return execute_query(query, fetch_results=True)

@app.get("/employees/age/total")
def get_total_employees_by_age():
    query = """
    SELECT *, (SELECT COUNT(*) FROM Airport.Employees WHERE Age = E.Age) AS TotalEmployees
    FROM (
        SELECT *, DATEDIFF(YEAR, BirthDate, GETDATE()) AS Age FROM Airport.Employees
    ) E
    ORDER BY E.Age;
    """
    return execute_query(query)

@app.get("/brigades/{brigade_id}/salary")
def get_brigade_salary(brigade_id: int):
    query = f"""
    SELECT B.BrigadeName, SUM(E.Salary) AS TotalSalary, AVG(E.Salary) AS AverageSalary
    FROM Airport.Brigades B
    JOIN Airport.Employees E ON B.BrigadeID = E.BrigadeID
    WHERE B.BrigadeID = {brigade_id}
    GROUP BY B.BrigadeName;
    """
    return execute_query(query)

@app.get("/pilots/medical_examination/{year}")
def get_pilots_medical_examination(year: int):
    query = f"""
    SELECT P.*,
           CASE WHEN ME.Passed = 1 THEN 'Прошедший' ELSE 'Не прошедший' END AS MedicalCheckStatus
    FROM Airport.Pilots P
    LEFT JOIN Airport.MedicalExaminations ME ON P.PilotID = ME.PilotID AND YEAR(ME.ExaminationDate) = {year};
    """
    return execute_query(query)

@app.get("/pilots/gender/{year}")
def get_pilots_by_gender(year: int):
    query = f"""
    SELECT E.Gender, COUNT(*) AS TotalPilots
    FROM Airport.Pilots P
    LEFT JOIN Airport.MedicalExaminations ME ON P.PilotID = ME.PilotID AND YEAR(ME.ExaminationDate) = {year}
    LEFT JOIN Airport.Employees E ON P.EmployeeID = E.EmployeeID
    GROUP BY E.Gender;
    """
    return execute_query(query)

@app.get("/pilots/age/{year}")
def get_pilots_by_age(year: int):
    query = f"""
    SELECT CASE
               WHEN DATEDIFF(YEAR, E.BirthDate, GETDATE()) BETWEEN 18 AND 29 THEN '18-29'
               WHEN DATEDIFF(YEAR, E.BirthDate, GETDATE()) BETWEEN 30 AND 39 THEN '30-39'
               WHEN DATEDIFF(YEAR, E.BirthDate, GETDATE()) BETWEEN 40 AND 49 THEN '40-49'
               WHEN DATEDIFF(YEAR, E.BirthDate, GETDATE()) BETWEEN 50 AND 59 THEN '50-59'
               ELSE '60+'
           END AS AgeGroup,
           COUNT(*) AS TotalPilots
    FROM Airport.Pilots P
    LEFT JOIN Airport.MedicalExaminations ME ON P.PilotID = ME.PilotID AND YEAR(ME.ExaminationDate) = {year}
    LEFT JOIN Airport.Employees E ON P.EmployeeID = E.EmployeeID
    GROUP BY CASE
                 WHEN DATEDIFF(YEAR, E.BirthDate, GETDATE()) BETWEEN 18 AND 29 THEN '18-29'
                 WHEN DATEDIFF(YEAR, E.BirthDate, GETDATE()) BETWEEN 30 AND 39 THEN '30-39'
                 WHEN DATEDIFF(YEAR, E.BirthDate, GETDATE()) BETWEEN 40 AND 49 THEN '40-49'
                 WHEN DATEDIFF(YEAR, E.BirthDate, GETDATE()) BETWEEN 50 AND 59 THEN '50-59'
                 ELSE '60+'
             END;
    """
    return execute_query(query)

@app.get("/pilots/salary/{year}")
def get_pilots_by_salary(year: int):
    query = f"""
    SELECT CASE
               WHEN E.Salary <= 30000 THEN 'Менее 30,000'
               WHEN E.Salary BETWEEN 30001 AND 50000 THEN '30,001-50,000'
               WHEN E.Salary BETWEEN 50001 AND 80000 THEN '50,001-80,000'
               ELSE 'Более 80,000'
           END AS SalaryRange,
           COUNT(*) AS TotalPilots
    FROM Airport.Pilots P
    LEFT JOIN Airport.MedicalExaminations ME ON P.PilotID = ME.PilotID AND YEAR(ME.ExaminationDate) = {year}
    LEFT JOIN Airport.Employees E ON P.EmployeeID = E.EmployeeID
    GROUP BY CASE
                 WHEN E.Salary <= 30000 THEN 'Менее 30,000'
                 WHEN E.Salary BETWEEN 30001 AND 50000 THEN '30,001-50,000'
                 WHEN E.Salary BETWEEN 50001 AND 80000 THEN '50,001-80,000'
                 ELSE 'Более 80,000'
             END;
    """
    return execute_query(query)

@app.get("/aircrafts/total_at_time/{datetime}")
def get_aircrafts_total_at_time(datetime: str):
    query = f"""
    DECLARE @DateTime DATETIME = '{datetime}';
    SELECT A.*,
           (SELECT COUNT(*)
            FROM Airport.Aircrafts
            WHERE ArrivalDate <= @DateTime AND InFlight = 0) AS TotalAircrafts
    FROM Airport.Aircrafts A
    WHERE A.ArrivalDate = @DateTime
    AND A.InFlight = 0;
    """
    return execute_query(query)

@app.get("/aircrafts/total_arrival/{datetime}")
def get_aircrafts_total_arrival(datetime: str):
    query = f"""
    DECLARE @DateTime DATETIME = '{datetime}';
    SELECT A.*,
           (SELECT COUNT(*)
            FROM Airport.Aircrafts
            WHERE ArrivalDate <= @DateTime) AS TotalAircrafts
    FROM Airport.Aircrafts A
    WHERE ArrivalDate <= @DateTime;
    """
    return execute_query(query)

@app.get("/aircrafts/total_flights/{total_flights}")
def get_aircrafts_total_flights(total_flights: int):
    query = f"""
    DECLARE @TargetTotalFlights INT = {total_flights};
    SELECT A.*,
           (SELECT COUNT(*)
            FROM Airport.Aircrafts
            WHERE TotalFlights = @TargetTotalFlights) AS TotalAircrafts
    FROM Airport.Aircrafts A
    WHERE A.TotalFlights = @TargetTotalFlights
    """
    return execute_query(query)

# ... остальные новые маршруты ...

@app.get("/flights/total_by_route_price/{route}/{ticket_price}")
def get_flights_total_by_route_price(route: str, ticket_price: float):
    query = f"""
    DECLARE @Route NVARCHAR(255) = '{route}';
    DECLARE @TicketPrice DECIMAL(10, 2) = {ticket_price};
    SELECT F.*,
           (SELECT COUNT(*)
            FROM Airport.Flights
            WHERE Route = @Route
            AND TicketPrice = @TicketPrice) AS TotalFlights
    FROM Airport.Flights F
    WHERE Route = @Route
    AND TicketPrice = @TicketPrice;
    """
    return execute_query(query)


@app.get("/aircrafts/total_repairs/{num_repairs}/{start_date}/{end_date}")
def get_aircrafts_total_repairs(num_repairs: int, start_date: str, end_date: str):
    query = f"""
    DECLARE @StartDate DATETIME = '{start_date}';
    DECLARE @EndDate DATETIME = '{end_date}';
    DECLARE @NumRepairs INT = {num_repairs};
    SELECT A.AircraftID, COUNT(*) AS TotalRepairs
    FROM Airport.Aircrafts AS A
    JOIN Airport.AircraftsRepairs AS AR ON A.AircraftID = AR.AircraftID
    WHERE AR.RepairDate BETWEEN @StartDate AND @EndDate
    GROUP BY A.AircraftID
    HAVING COUNT(*) >= @NumRepairs;
    """
    return execute_query(query)

@app.get("/aircrafts/by_age/{min_age}")
def get_aircrafts_by_age(min_age: int):
    query = f"""
    DECLARE @MinAge INT = {min_age};
    SELECT *
    FROM Airport.Aircrafts
    WHERE Age >= @MinAge;
    """
    return execute_query(query)

@app.get("/aircrafts/flights_before_repair/{repair_date}")
def get_aircrafts_flights_before_repair(repair_date: str):
    query = f"""
    DECLARE @RepairDate DATETIME = '{repair_date}';
    SELECT A.*, F.TotalFlights
    FROM Airport.Aircrafts AS A
    JOIN (
        SELECT AircraftID, COUNT(*) AS TotalFlights
        FROM Airport.Flights AS F
        WHERE F.DepartureDateTime < @RepairDate
        GROUP BY AircraftID
    ) AS F ON A.AircraftID = F.AircraftID;
    """
    return execute_query(query)

# ... остальные новые маршруты ...

@app.get("/flights/cancelled/{route}")
def get_flights_cancelled(route: str):
    query = f"""
    DECLARE @Route NVARCHAR(255) = '{route}';
    SELECT CF.CancelledFlightID, CF.FlightID, CF.CancellationReason,
           SUM(CF.UnusedSeats) AS TotalUnusedSeats,
           AVG(CAST(CF.UnusedSeats AS DECIMAL) / (CAST(TB.ReservedSeats AS DECIMAL) + CF.UnusedSeats) * 100) AS AverageUnusedSeatsPercentage
    FROM Airport.CancelledFlights CF
    JOIN Airport.Flights F ON CF.FlightID = F.FlightID
    JOIN Airport.TicketBookings TB ON F.FlightID = TB.FlightID
    WHERE F.Route = @Route
    GROUP BY CF.CancelledFlightID, CF.FlightID, CF.CancellationReason;
    """
    return execute_query(query)


@app.get("/flights/delayed_by_reason/{reason}/{route}")
def get_flights_delayed_by_reason(reason: str, route: str):
    query = f"""
    DECLARE @Reason NVARCHAR(255) = '{reason}';
    DECLARE @Route NVARCHAR(255) = '{route}';
    SELECT DF.DelayedFlightID, DF.FlightID, DF.DelayReason,
           COUNT(TB.BookingID) AS TotalReturnedTickets
    FROM Airport.DelayedFlights DF
    JOIN Airport.Flights F ON DF.FlightID = F.FlightID
    JOIN Airport.TicketBookings TB ON F.FlightID = TB.FlightID
    WHERE DF.DelayReason = @Reason
    AND F.Route = @Route
    GROUP BY DF.DelayedFlightID, DF.FlightID, DF.DelayReason;
    """
    return execute_query(query)

@app.get("/flights/average_sold_tickets/{aircraft_model}")
def get_flights_average_sold_tickets(aircraft_model: str):
    query = f"""
    DECLARE @AircraftModel NVARCHAR(255) = '{aircraft_model}';
    SELECT F.DepartureDateTime,
           F.ArrivalDateTime,
           F.Route,
           DATEDIFF(HOUR, F.DepartureDateTime, F.ArrivalDateTime) AS DurationHours,
           AVG(F.TicketPrice) AS AverageTicketPrice,
           AVG(TB.SoldTickets) AS AverageSoldTickets
    FROM Airport.Flights F
    JOIN Airport.Aircrafts A ON F.AircraftID = A.AircraftID
    JOIN (
        SELECT FlightID, SUM(ReservedSeats) AS SoldTickets
        FROM Airport.TicketBookings
        GROUP BY FlightID
    ) TB ON F.FlightID = TB.FlightID
    WHERE A.ModelID = (SELECT ModelID FROM Airport.AircraftModels WHERE ModelName = @AircraftModel)
    GROUP BY F.DepartureDateTime, F.ArrivalDateTime, F.Route;
    """
    return execute_query(query)

@app.get("/flights/by_type_and_model/{flight_type}/{route}/{aircraft_model}")
def get_flights_by_type_and_model(flight_type: str, route: str, aircraft_model: str):
    query = f"""
    DECLARE @FlightType NVARCHAR(255) = '{flight_type}';
    DECLARE @Route NVARCHAR(255) = '{route}';
    DECLARE @AircraftModel NVARCHAR(255) = '{aircraft_model}';
    DECLARE @FlightTypeID INT;
    SELECT @FlightTypeID = FlightTypeID
    FROM Airport.FlightTypes
    WHERE FlightTypeName = @FlightType;

    SELECT COUNT(*) AS TotalFlights,
           F.*
    FROM Airport.Flights F
    JOIN Airport.Aircrafts A ON F.AircraftID = A.AircraftID
    JOIN Airport.AircraftModels AM ON A.ModelID = AM.ModelID
    WHERE F.FlightTypeID = @FlightTypeID
    AND F.Route = @Route
    AND AM.ModelID = (SELECT ModelID FROM Airport.AircraftModels WHERE ModelName = @AircraftModel)
    GROUP BY FlightID, F.AircraftID, FlightTypeID, DepartureDateTime, ArrivalDateTime, Route, TicketPrice, Status;
    """
    return execute_query(query)


@app.get("/passengers/by_flight_and_date/{flight_id}/{date}")
def get_passengers_by_flight_and_date(flight_id: int, date: str):
    query = f"""
    DECLARE @FlightID INT = {flight_id};
    DECLARE @Date DATE = '{date}';
    SELECT
        P.PassengerID,
        P.FirstName,
        P.LastName,
        P.Gender,
        P.BirthDate,
        T.IsLuggage,
        T.IsCheckedIn,
        T.PurchaseDate
    FROM
        Airport.Tickets T
    JOIN
        Airport.Passengers P ON T.PassengerID = P.PassengerID
    JOIN
        Airport.Flights F ON T.FlightID = F.FlightID
    WHERE
        F.FlightID = @FlightID
        AND F.DepartureDateTime BETWEEN @Date AND DATEADD(DAY, 1, @Date);
    """
    return execute_query(query)

@app.get("/flights/booked_and_available_seats/{flight_id}")
def get_flights_booked_and_available_seats(flight_id: int):
    query = f"""
    DECLARE @FlightID INT = {flight_id};
    SELECT
        F.FlightID,
        TB.ReservedSeats AS BookedSeats,
        TB.LeftSeats AS AvailableSeats
    FROM
        Airport.Flights F
    LEFT JOIN
        Airport.TicketBookings TB ON F.FlightID = TB.FlightID
    WHERE
        F.FlightID = @FlightID;
    """
    return execute_query(query)

@app.get("/tickets/returned_by_flight_and_date/{flight_id}/{date}/{route}/{ticket_price}/{min_age}/{max_age}/{gender}")
def get_tickets_returned_by_flight_and_date(flight_id: int, date: str, route: str, ticket_price: float, min_age: int, max_age: int, gender: str):
    query = f"""
    DECLARE @FlightID INT = {flight_id};
    DECLARE @Date DATE = '{date}';
    DECLARE @Route NVARCHAR(255) = '{route}';
    DECLARE @TicketPrice DECIMAL(10, 2) = {ticket_price};
    DECLARE @MinAge INT = {min_age};
    DECLARE @MaxAge INT = {max_age};
    DECLARE @Gender NVARCHAR(10) = '{gender}';

    SELECT
    COUNT(T.TicketID) AS ReturnedTicketsCount,
    P.Gender,
    DATEDIFF(YEAR, P.BirthDate, @Date) AS Age
FROM
    Airport.Tickets T
JOIN
    Airport.Passengers P ON T.PassengerID = P.PassengerID
JOIN
    Airport.Flights F ON T.FlightID = F.FlightID
WHERE
    F.FlightID = @FlightID
    AND F.DepartureDateTime BETWEEN @Date AND DATEADD(DAY, 1, @Date)
    AND F.Route = @Route
    AND F.TicketPrice = @TicketPrice
    AND DATEDIFF(YEAR, P.BirthDate, @Date) BETWEEN @MinAge AND @MaxAge
    AND P.Gender = @Gender
    AND T.IsCheckedIn = 0 -- Предполагается, что сданные билеты имеют IsCheckedIn = 0
GROUP BY
    P.Gender,
    DATEDIFF(YEAR, P.BirthDate, @Date);
    """
    return execute_query(query)

# Модель данных для ввода
class Pilot(BaseModel):
    employee_id: int
#если не будет работать сделать гет запросом
@app.post("/add/pilots/")
def add_pilot(pilot:Pilot):
    query_insert = f"""
    DECLARE @EmployeeID INT = {pilot.employee_id};
    INSERT INTO Airport.Pilots (EmployeeID)
    VALUES (@EmployeeID)
    """
    execute_query(query_insert)
    # Предполагается, что у пилотов есть уникальный идентификатор PilotID
    query_select = f"""
        SELECT p.*, e.*
        FROM Airport.Pilots p
        INNER JOIN Airport.Employees e ON p.EmployeeID = e.EmployeeID
        WHERE p.EmployeeID = {pilot.employee_id}
        """
    return execute_query(query_select, fetch_results=True)

@app.delete("/pilots/{pilotID}")
def delete_pilot(pilotID: int):
    query = f"DELETE FROM Airport.Pilots WHERE PilotID = {pilotID};"
    result = execute_query(query)
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=1488, reload=True)

@app.get("/huesos/ebaniy")
def huesos_ebaniy():
    return {"message": "Ebaniy"}

@app.get("/starie/yeban/xvatit/zadanie/neironkoi/generit")
def starie_yeban_xvatit_zadanie_neironkoi_generit():
    file_path = "static/dedula.gif"
    if os.path.exists(file_path):
        return FileResponse(file_path, media_type='image/gif')
    else:
        raise HTTPException(status_code=404, detail="File not found")
