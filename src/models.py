from sqlalchemy import (
    Column, String, Integer, Date, Numeric,
    ForeignKey, UniqueConstraint, CheckConstraint
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Клиент(Base):
    __tablename__ = 'клиенты'

    лицевой_счет = Column(String(20), primary_key=True)
    фио = Column(String(100), nullable=False)
    адрес = Column(String(255), nullable=False)
    телефон = Column(String(20))

    платежи = relationship("Платеж", back_populates="клиент")
    потребление = relationship("Потребление", back_populates="клиент")


class Услуга(Base):
    __tablename__ = 'услуги'

    код_услуги = Column(Integer, primary_key=True)
    наименование = Column(String(100), nullable=False)
    единица_измерения = Column(String(20), nullable=False)

    тарифы = relationship("Тариф", back_populates="услуга")
    платежи = relationship("Платеж", back_populates="услуга")
    потребление = relationship("Потребление", back_populates="услуга")


class Платеж(Base):
    __tablename__ = 'платежи'

    код_платежа = Column(Integer, primary_key=True)
    лицевой_счет = Column(String(20), ForeignKey('клиенты.лицевой_счет'), nullable=False)
    код_услуги = Column(Integer, ForeignKey('услуги.код_услуги'), nullable=False)
    период = Column(Date, nullable=False)
    дата_платежа = Column(Date, nullable=False)
    сумма_платежа = Column(Numeric(10, 2), nullable=False)

    клиент = relationship("Клиент", back_populates="платежи")
    услуга = relationship("Услуга", back_populates="платежи")


class Потребление(Base):
    __tablename__ = 'потребление'

    код_потребления = Column(Integer, primary_key=True)
    лицевой_счет = Column(String(20), ForeignKey('клиенты.лицевой_счет'), nullable=False)
    код_услуги = Column(Integer, ForeignKey('услуги.код_услуги'), nullable=False)
    период = Column(Date, nullable=False)
    объем = Column(Numeric(10, 2), nullable=False)
    сумма_к_оплате = Column(Numeric(10, 2), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            'лицевой_счет',
            'код_услуги',
            'период',
            name='уникальное_потребление'
        ),
    )

    клиент = relationship("Клиент", back_populates="потребление")
    услуга = relationship("Услуга", back_populates="потребление")


class Тариф(Base):
    __tablename__ = 'тарифы'

    код_тарифа = Column(Integer, primary_key=True)
    код_услуги = Column(Integer, ForeignKey('услуги.код_услуги'), nullable=False)
    тарифная_зона = Column(String(50), nullable=False)
    стоимость_единицы = Column(Numeric(10, 2), nullable=False)
    действует_с = Column(Date, nullable=False)
    действует_по = Column(Date)

    __table_args__ = (
        UniqueConstraint(
            'код_услуги',
            'тарифная_зона',
            'действует_по',
            name='уникальный_текущий_тариф'
        ),
    )

    услуга = relationship("Услуга", back_populates="тарифы")
