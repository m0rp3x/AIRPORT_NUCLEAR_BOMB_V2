namespace BlazorApp4.Data;

public class Classes
{
    public class Pilot
    {
        public int employee_id { get; set; }
        private int pilot_id;
    }
}
public class ApiPostMethod
{
    public Type ObjectType { get; set; } // Тип объекта, который нужно передать
    public string Endpoint { get; set; } // Эндпоинт для отправки запроса

    // Конструктор для удобства инициализации
    public ApiPostMethod(string name, Type objectType, string endpoint)
    {
        ObjectType = objectType;
        Endpoint = endpoint;
    }
}